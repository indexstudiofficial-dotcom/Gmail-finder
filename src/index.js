// ============================================================
// GMAIL INBOX COLLECTOR
// Cloudflare Workers + Supabase + Composio
//
// WORKFLOW
// 1. Receive a POST request from Supabase Cron.
// 2. Select the oldest-checked connected Gmail integration.
// 3. Fetch unread emails received within the last 24 hours.
// 4. Save emails to gmail_processed_messages.
// 5. Mark successfully saved emails as read.
// 6. Update last_checked_at.
//
// NO AI REPLIES
// NO GMAIL DRAFTS
// NO OUTBOUND EMAILS
// ============================================================


// ============================================================
// CONFIGURATION
// ============================================================

const MAX_EMAILS_PER_PAGE = 100;
const MAX_PAGES_PER_RUN = 5;


// ============================================================
// MAIN WORKER
// ============================================================

export default {
  async fetch(request, env) {
    // --------------------------------------------------------
    // 1. Accept POST requests only
    // --------------------------------------------------------

    if (request.method !== "POST") {
      return jsonResponse(
        {
          success: false,
          error: "Only POST requests are supported."
        },
        405
      );
    }

    // --------------------------------------------------------
    // 2. Validate environment variables
    // --------------------------------------------------------

    const required = [
      "SUPABASE_URL",
      "SUPABASE_SERVICE_ROLE_KEY",
      "COMPOSIO_API_KEY"
    ];

    const missing = required.filter((key) => !env[key]);

    if (missing.length > 0) {
      return jsonResponse(
        {
          success: false,
          error: "Missing Worker environment variables.",
          missing
        },
        500
      );
    }

    // --------------------------------------------------------
    // 3. Select oldest-checked Gmail integration
    // --------------------------------------------------------

    let integration;

    try {
      integration = await getOldestGmailIntegration(env);
    } catch (error) {
      console.error("Integration lookup failed:", error.message);

      return jsonResponse(
        {
          success: false,
          error: "Could not retrieve Gmail integrations."
        },
        500
      );
    }

    if (!integration) {
      return jsonResponse({
        success: true,
        message: "No connected Gmail integrations found.",
        accounts_checked: 0,
        emails_saved: 0
      });
    }

    // --------------------------------------------------------
    // 4. Initialize processing statistics
    // --------------------------------------------------------

    const stats = {
      integration_id: integration.id,
      emails_found: 0,
      emails_saved: 0,
      duplicates: 0,
      emails_marked_read: 0,
      email_errors: 0,
      pages_processed: 0
    };

    let processingError = null;

    // --------------------------------------------------------
    // 5. Process selected Gmail account
    // --------------------------------------------------------

    try {
      await processGmailAccount(
        env,
        integration,
        stats
      );
    } catch (error) {
      processingError = error.message;

      console.error("Gmail processing failed:", {
        integration_id: integration.id,
        error: error.message
      });
    } finally {
      // ------------------------------------------------------
      // Update timestamp after every processing attempt.
      // ------------------------------------------------------

      try {
        await updateLastCheckedAt(env, integration.id);
      } catch (error) {
        console.error(
          "last_checked_at update failed:",
          error.message
        );

        processingError = processingError
          ? `${processingError}; timestamp update failed: ${error.message}`
          : `Timestamp update failed: ${error.message}`;
      }
    }

    // --------------------------------------------------------
    // 6. Return results
    // --------------------------------------------------------

    return jsonResponse(
      {
        success: !processingError,
        message: processingError
          ? "Gmail processing encountered an error."
          : "Gmail processing completed.",
        account_id: integration.id,
        application_id: integration.application_id,
        stats,
        error: processingError
      },
      processingError ? 500 : 200
    );
  }
};


// ============================================================
// SUPABASE REST API
// ============================================================

async function supabaseRequest(
  env,
  path,
  options = {}
) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const response = await fetch(
    `${baseUrl}/rest/v1/${path}`,
    {
      ...options,
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization:
          `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        ...(options.headers || {})
      }
    }
  );

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Supabase HTTP ${response.status}: ${text}`
    );
  }

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}


// ============================================================
// FIND OLDEST-CHECKED GMAIL INTEGRATION
// ============================================================

async function getOldestGmailIntegration(env) {
  const query = new URLSearchParams({
    select:
      "id,user_id,application_id,connection_id,integration_id,status,last_checked_at",
    integration_id: "eq.gmail",
    status: "eq.connected",
    order: "last_checked_at.asc.nullsfirst",
    limit: "1"
  });

  const rows = await supabaseRequest(
    env,
    `user_integrations?${query.toString()}`
  );

  if (!Array.isArray(rows) || rows.length === 0) {
    return null;
  }

  return rows[0];
}


// ============================================================
// PROCESS ONE GMAIL ACCOUNT
// ============================================================

async function processGmailAccount(
  env,
  integration,
  stats
) {
  let pageToken = null;

  for (
    let page = 0;
    page < MAX_PAGES_PER_RUN;
    page++
  ) {
    // --------------------------------------------------------
    // Gmail search:
    // unread emails received within the last 24 hours
    // --------------------------------------------------------

    const parameters = [
      {
        name: "q",
        value: "is:unread newer_than:1d",
        in: "query",
        type: "string"
      },
      {
        name: "maxResults",
        value: String(MAX_EMAILS_PER_PAGE),
        in: "query",
        type: "string"
      }
    ];

    if (pageToken) {
      parameters.push({
        name: "pageToken",
        value: pageToken,
        in: "query",
        type: "string"
      });
    }

    // --------------------------------------------------------
    // Fetch message IDs through Composio
    // --------------------------------------------------------

    const result = await composioProxy(
      env,
      integration,
      {
        endpoint: "/users/me/messages",
        method: "GET",
        parameters
      }
    );

    const messages = result?.messages || [];

    stats.pages_processed++;
    stats.emails_found += messages.length;

    // --------------------------------------------------------
    // Process each message individually
    // --------------------------------------------------------

    for (const item of messages) {
      if (!item.id) {
        stats.email_errors++;
        continue;
      }

      try {
        const outcome = await processSingleEmail(
          env,
          integration,
          item.id
        );

        if (outcome === "saved") {
          stats.emails_saved++;
        } else if (outcome === "duplicate") {
          stats.duplicates++;
        }

        if (outcome !== "failed") {
          stats.emails_marked_read++;
        }
      } catch (error) {
        stats.email_errors++;

        console.error("Message processing failed:", {
          gmail_message_id: item.id,
          integration_id: integration.id,
          error: error.message
        });

        // Do not mark an email as read if saving or
        // verifying its database record failed.
      }
    }

    pageToken = result?.nextPageToken || null;

    if (!pageToken) {
      break;
    }
  }
}


// ============================================================
// FETCH AND SAVE ONE EMAIL
// ============================================================

async function processSingleEmail(
  env,
  integration,
  messageId
) {
  // ----------------------------------------------------------
  // 1. Retrieve complete message from Gmail
  // ----------------------------------------------------------

  const message = await composioProxy(
    env,
    integration,
    {
      endpoint:
        `/users/me/messages/${encodeURIComponent(messageId)}`,
      method: "GET",
      parameters: [
        {
          name: "format",
          value: "full",
          in: "query",
          type: "string"
        }
      ]
    }
  );

  if (!message?.id) {
    throw new Error("Gmail returned no message ID.");
  }

  const headers = message.payload?.headers || [];

  const from = getHeader(headers, "from");
  const to = getHeader(headers, "to");
  const date = getHeader(headers, "date");

  const sender = parseEmailAddress(from);
  const recipient = parseEmailAddress(to);

  const receivedAt = message.internalDate
    ? new Date(
        Number(message.internalDate)
      ).toISOString()
    : parseEmailDate(date);

  const emailContent =
    extractEmailBody(message.payload) ||
    message.snippet ||
    "";

  // ----------------------------------------------------------
  // 2. Create database record
  // ----------------------------------------------------------

  const record = {
    user_id: integration.user_id,
    application_id: integration.application_id || null,
    connected_account_id: integration.connection_id,
    gmail_message_id: message.id,

    sender_email: sender.email || null,
    sender_name: sender.name || null,
    recipient_email: recipient.email || null,

    email_content: emailContent,
    email_received_at: receivedAt,

    email_category: null,
    should_reply: null,

    ai_draft_reply: null,
    gmail_draft_id: null,

    status: "pending",
    error_message: null,
    processed_at: null
  };

  // ----------------------------------------------------------
  // 3. Insert without overwriting existing records
  //
  // Requires a unique constraint on:
  // (connected_account_id, gmail_message_id)
  // ----------------------------------------------------------

  const query = new URLSearchParams({
    on_conflict: "connected_account_id,gmail_message_id"
  });

  const inserted = await supabaseRequest(
    env,
    `gmail_processed_messages?${query.toString()}`,
    {
      method: "POST",
      headers: {
        Prefer: "resolution=ignore-duplicates,return=representation"
      },
      body: JSON.stringify(record)
    }
  );

  // When inserted, Supabase returns the inserted record.
  // When an existing record is ignored, the array is empty.

  const wasInserted =
    Array.isArray(inserted) && inserted.length > 0;

  // ----------------------------------------------------------
  // 4. Mark email as read only after the database operation
  // succeeds.
  //
  // Existing duplicate records are also safe to mark read.
  // ----------------------------------------------------------

  await composioProxy(
    env,
    integration,
    {
      endpoint:
        `/users/me/messages/${encodeURIComponent(message.id)}/modify`,
      method: "POST",
      body: {
        removeLabelIds: ["UNREAD"]
      }
    }
  );

  return wasInserted ? "saved" : "duplicate";
}


// ============================================================
// COMPOSIO PROXY REQUEST
// ============================================================

async function composioProxy(
  env,
  integration,
  requestOptions
) {
  const payload = {
    connected_account_id: integration.connection_id,
    endpoint: requestOptions.endpoint,
    method: requestOptions.method
  };

  // ----------------------------------------------------------
  // Add query parameters or headers.
  //
  // IMPORTANT FIX:
  // Each parameter now includes the required "type" property.
  // ----------------------------------------------------------

  if (requestOptions.parameters) {
    payload.parameters = requestOptions.parameters.map(
      (parameter) => ({
        ...parameter,
        type: parameter.type || "string"
      })
    );
  }

  if (requestOptions.body !== undefined) {
    payload.body = requestOptions.body;
  }

  // ----------------------------------------------------------
  // Send request to Composio
  // ----------------------------------------------------------

  const response = await fetch(
    "https://backend.composio.dev/api/v3.1/tools/execute/proxy",
    {
      method: "POST",
      headers: {
        "x-api-key": env.COMPOSIO_API_KEY,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    }
  );

  const responseText = await response.text();

  let result;

  try {
    result = responseText
      ? JSON.parse(responseText)
      : {};
  } catch {
    throw new Error(
      `Composio returned invalid JSON: ${responseText}`
    );
  }

  // ----------------------------------------------------------
  // Check Composio HTTP response
  // ----------------------------------------------------------

  if (!response.ok) {
    throw new Error(
      `Composio HTTP ${response.status}: ${
        JSON.stringify(result)
      }`
    );
  }

  // ----------------------------------------------------------
  // Check the actual Gmail API response status.
  //
  // Composio can return HTTP 200 even when Gmail returns
  // a non-2xx status in result.status.
  // ----------------------------------------------------------

  if (
    typeof result.status === "number" &&
    (result.status < 200 || result.status >= 300)
  ) {
    throw new Error(
      `Gmail API HTTP ${result.status}: ${
        JSON.stringify(result.data)
      }`
    );
  }

  if (result.error) {
    throw new Error(
      `Composio error: ${JSON.stringify(result.error)}`
    );
  }

  return result.data ?? result;
}


// ============================================================
// READ EMAIL HEADERS
// ============================================================

function getHeader(headers, name) {
  const header = headers.find(
    (item) =>
      item.name?.toLowerCase() === name.toLowerCase()
  );

  return header?.value || "";
}


// ============================================================
// PARSE EMAIL ADDRESS
// ============================================================

function parseEmailAddress(value) {
  if (!value) {
    return {
      name: "",
      email: ""
    };
  }

  const match = value.match(/^(.*?)\s*<([^<>]+)>$/);

  if (match) {
    return {
      name: match[1]
        .trim()
        .replace(/^["']|["']$/g, ""),
      email: match[2].trim()
    };
  }

  const emailMatch = value.match(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i
  );

  return {
    name: "",
    email: emailMatch ? emailMatch[0] : value.trim()
  };
}


// ============================================================
// DECODE GMAIL BASE64URL BODY
// ============================================================

function decodeBase64Url(data) {
  if (!data) {
    return "";
  }

  const normalized = data
    .replace(/-/g, "+")
    .replace(/_/g, "/");

  const padded =
    normalized +
    "=".repeat((4 - normalized.length % 4) % 4);

  try {
    const binary = atob(padded);

    const bytes = Uint8Array.from(
      binary,
      (character) => character.charCodeAt(0)
    );

    return new TextDecoder().decode(bytes);
  } catch {
    return "";
  }
}


// ============================================================
// EXTRACT EMAIL BODY
// Prefer text/plain; otherwise use text/html.
// ============================================================

function extractEmailBody(payload) {
  if (!payload) {
    return "";
  }

  const plainParts = [];
  const htmlParts = [];

  function walk(part) {
    if (!part) {
      return;
    }

    const mimeType =
      (part.mimeType || "").toLowerCase();

    if (part.body?.data) {
      const decoded = decodeBase64Url(
        part.body.data
      );

      if (mimeType === "text/plain") {
        plainParts.push(decoded);
      } else if (mimeType === "text/html") {
        htmlParts.push(decoded);
      }
    }

    if (Array.isArray(part.parts)) {
      for (const child of part.parts) {
        walk(child);
      }
    }
  }

  walk(payload);

  if (plainParts.length > 0) {
    return plainParts.join("\n").trim();
  }

  if (htmlParts.length > 0) {
    return htmlParts
      .join("\n")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/(div|p|li|tr|h[1-6])>/gi, "\n")
      .replace(/<[^>]*>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/gi, "'")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  }

  return "";
}


// ============================================================
// PARSE EMAIL DATE
// ============================================================

function parseEmailDate(value) {
  if (!value) {
    return null;
  }

  const timestamp = Date.parse(value);

  return Number.isNaN(timestamp)
    ? null
    : new Date(timestamp).toISOString();
}


// ============================================================
// UPDATE LAST-CHECKED TIMESTAMP
// ============================================================

async function updateLastCheckedAt(
  env,
  integrationId
) {
  const query = new URLSearchParams({
    id: `eq.${integrationId}`,
    integration_id: "eq.gmail"
  });

  const now = new Date().toISOString();

  await supabaseRequest(
    env,
    `user_integrations?${query.toString()}`,
    {
      method: "PATCH",
      headers: {
        Prefer: "return=minimal"
      },
      body: {
        last_checked_at: now,
        updated_at: now
      }
        ? JSON.stringify({
            last_checked_at: now,
            updated_at: now
          })
        : undefined
    }
  );
}


// ============================================================
// JSON RESPONSE
// ============================================================

function jsonResponse(data, status = 200) {
  return new Response(
    JSON.stringify(data, null, 2),
    {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store"
      }
    }
  );
          }
