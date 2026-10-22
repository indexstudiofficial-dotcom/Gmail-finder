// ============================================================
// GMAIL INBOX COLLECTOR
// Cloudflare Worker + Supabase + Composio
//
// Responsibilities:
// 1. Select the oldest-checked connected Gmail account.
// 2. Fetch unread emails received within the last 24 hours.
// 3. Save email information in gmail_processed_messages.
// 4. Mark successfully saved emails as read.
// 5. Update last_checked_at after the processing attempt.
//
// This Worker does NOT:
// - Generate AI replies.
// - Create Gmail drafts.
// - Send emails.
// ============================================================


// ============================================================
// CONFIGURATION
// ============================================================

const MAX_EMAILS_PER_PAGE = 100;
const MAX_PAGES_PER_RUN = 5;


// ============================================================
// MAIN REQUEST HANDLER
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
          error: "Method not allowed. Use POST."
        },
        405
      );
    }

    // --------------------------------------------------------
    // 2. Validate required environment variables
    // --------------------------------------------------------

    const requiredVariables = [
      "SUPABASE_URL",
      "SUPABASE_SERVICE_ROLE_KEY",
      "COMPOSIO_API_KEY"
    ];

    const missingVariables = requiredVariables.filter(
      (name) => !env[name]
    );

    if (missingVariables.length > 0) {
      return jsonResponse(
        {
          success: false,
          error: "Missing required Worker environment variables.",
          missing: missingVariables
        },
        500
      );
    }

    // --------------------------------------------------------
    // 3. Find the oldest-checked connected Gmail integration
    // --------------------------------------------------------

    let integration;

    try {
      integration = await getOldestGmailIntegration(env);
    } catch (error) {
      console.error("Failed to find Gmail integration:", error);

      return jsonResponse(
        {
          success: false,
          error: "Could not retrieve Gmail integrations."
        },
        500
      );
    }

    // --------------------------------------------------------
    // 4. Stop if no connected Gmail integration exists
    // --------------------------------------------------------

    if (!integration) {
      return jsonResponse({
        success: true,
        message: "No connected Gmail integrations need checking.",
        accounts_checked: 0,
        emails_saved: 0
      });
    }

    // --------------------------------------------------------
    // 5. Process the selected Gmail account
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

    let accountError = null;

    try {
      await processGmailAccount(
        env,
        integration,
        stats
      );
    } catch (error) {
      accountError = error.message || "Unknown processing error";

      console.error("Gmail account processing failed:", {
        integration_id: integration.id,
        error: accountError
      });
    } finally {
      // ------------------------------------------------------
      // 6. Update last_checked_at after every processing attempt
      // ------------------------------------------------------

      try {
        await updateLastCheckedAt(env, integration.id);
      } catch (error) {
        console.error(
          "Failed to update last_checked_at:",
          error
        );

        accountError = accountError
          ? `${accountError}; timestamp update failed`
          : "Gmail processing finished, but last_checked_at could not be updated.";
      }
    }

    // --------------------------------------------------------
    // 7. Return execution results
    // --------------------------------------------------------

    return jsonResponse(
      {
        success: !accountError,
        message: accountError
          ? "Gmail account processing encountered an error."
          : "Gmail account processing completed.",
        account_id: integration.id,
        application_id: integration.application_id,
        stats,
        error: accountError
      },
      accountError ? 500 : 200
    );
  }
};


// ============================================================
// SUPABASE REST API HELPER
// ============================================================

async function supabaseRequest(
  env,
  tablePath,
  options = {}
) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const response = await fetch(
    `${baseUrl}/rest/v1/${tablePath}`,
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

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Supabase HTTP ${response.status}: ${responseText}`
    );
  }

  if (!responseText) {
    return null;
  }

  try {
    return JSON.parse(responseText);
  } catch {
    return responseText;
  }
}


// ============================================================
// FIND THE OLDEST-CHECKED GMAIL INTEGRATION
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

  const integrations = await supabaseRequest(
    env,
    `user_integrations?${query.toString()}`
  );

  if (!Array.isArray(integrations) || integrations.length === 0) {
    return null;
  }

  return integrations[0];
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

  // Fetch unread messages received within the last 24 hours.
  // Pagination ensures the Worker can process multiple pages.

  for (
    let pageNumber = 0;
    pageNumber < MAX_PAGES_PER_RUN;
    pageNumber++
  ) {
    const parameters = [
      {
        name: "q",
        value: "is:unread newer_than:1d",
        in: "query"
      },
      {
        name: "maxResults",
        value: String(MAX_EMAILS_PER_PAGE),
        in: "query"
      }
    ];

    if (pageToken) {
      parameters.push({
        name: "pageToken",
        value: pageToken,
        in: "query"
      });
    }

    // --------------------------------------------------------
    // Fetch message IDs from Gmail
    // --------------------------------------------------------

    const listResult = await composioProxy(
      env,
      integration.connection_id,
      {
        endpoint: "/gmail/v1/users/me/messages",
        method: "GET",
        parameters
      }
    );

    const messages = listResult?.messages || [];

    stats.pages_processed++;
    stats.emails_found += messages.length;

    // --------------------------------------------------------
    // Process each unread email
    // --------------------------------------------------------

    for (const message of messages) {
      if (!message.id) {
        stats.email_errors++;
        continue;
      }

      try {
        await processSingleEmail(
          env,
          integration,
          message.id,
          stats
        );
      } catch (error) {
        stats.email_errors++;

        console.error("Email processing failed:", {
          integration_id: integration.id,
          gmail_message_id: message.id,
          error: error.message
        });

        // Do not mark this message as read when processing fails.
        // Gmail can return it during a later run.
      }
    }

    // --------------------------------------------------------
    // Continue if Gmail has another page
    // --------------------------------------------------------

    pageToken = listResult?.nextPageToken || null;

    if (!pageToken) {
      break;
    }
  }
}


// ============================================================
// PROCESS ONE EMAIL
// ============================================================

async function processSingleEmail(
  env,
  integration,
  messageId,
  stats
) {
  // ----------------------------------------------------------
  // 1. Fetch the complete Gmail message
  // ----------------------------------------------------------

  const message = await composioProxy(
    env,
    integration.connection_id,
    {
      endpoint:
        `/gmail/v1/users/me/messages/${encodeURIComponent(messageId)}`,
      method: "GET",
      parameters: [
        {
          name: "format",
          value: "full",
          in: "query"
        }
      ]
    }
  );

  const headers = getHeaders(message.payload);

  const fromHeader = getHeader(headers, "from");
  const toHeader = getHeader(headers, "to");
  const dateHeader = getHeader(headers, "date");

  const sender = parseEmailAddress(fromHeader);
  const recipient = parseEmailAddress(toHeader);

  const receivedAt = message.internalDate
    ? new Date(Number(message.internalDate)).toISOString()
    : parseEmailDate(dateHeader);

  let emailContent = extractEmailBody(message.payload);

  if (!emailContent) {
    emailContent = message.snippet || "";
  }

  // ----------------------------------------------------------
  // 2. Prepare the database record
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
    processed_at: null,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  };

  // ----------------------------------------------------------
  // 3. Insert the email, ignoring duplicates
  // ----------------------------------------------------------

  const query = new URLSearchParams({
    on_conflict: "connected_account_id,gmail_message_id"
  });

  await supabaseRequest(
    env,
    `gmail_processed_messages?${query.toString()}`,
    {
      method: "POST",
      headers: {
        Prefer: "resolution=ignore-duplicates,return=minimal"
      },
      body: JSON.stringify(record)
    }
  );

  // At this point the database insert succeeded, or the existing
  // unique record was safely ignored.
  //
  // Do not generate a draft or send a reply.

  stats.emails_saved++;

  // ----------------------------------------------------------
  // 4. Mark the Gmail message as read
  // ----------------------------------------------------------

  await composioProxy(
    env,
    integration.connection_id,
    {
      endpoint:
        `/gmail/v1/users/me/messages/${encodeURIComponent(message.id)}/modify`,
      method: "POST",
      body: {
        removeLabelIds: ["UNREAD"]
      }
    }
  );

  stats.emails_marked_read++;
}


// ============================================================
// COMPOSIO AUTHENTICATED GMAIL API REQUEST
// ============================================================

async function composioProxy(
  env,
  connectedAccountId,
  requestOptions
) {
  const response = await fetch(
    "https://backend.composio.dev/api/v3.1/tools/execute/proxy",
    {
      method: "POST",
      headers: {
        "x-api-key": env.COMPOSIO_API_KEY,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        connected_account_id: connectedAccountId,
        endpoint: requestOptions.endpoint,
        method: requestOptions.method,
        ...(requestOptions.parameters
          ? { parameters: requestOptions.parameters }
          : {}),
        ...(requestOptions.body !== undefined
          ? { body: requestOptions.body }
          : {})
      })
    }
  );

  const responseText = await response.text();

  let result;

  try {
    result = responseText
      ? JSON.parse(responseText)
      : {};
  } catch {
    result = {
      raw_response: responseText
    };
  }

  if (!response.ok) {
    throw new Error(
      `Composio HTTP ${response.status}: ${JSON.stringify(result)}`
    );
  }

  // The proxy can return an HTTP success while the underlying
  // Gmail API reports an error. Check its upstream status too.

  if (
    typeof result.status === "number" &&
    (result.status < 200 || result.status >= 300)
  ) {
    throw new Error(
      `Gmail API HTTP ${result.status}: ${JSON.stringify(result.data)}`
    );
  }

  if (result.error) {
    throw new Error(
      `Composio/Gmail error: ${JSON.stringify(result.error)}`
    );
  }

  return result.data ?? result;
}


// ============================================================
// READ GMAIL HEADERS
// ============================================================

function getHeaders(payload) {
  return payload?.headers || [];
}

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
      name: match[1].trim().replace(/^["']|["']$/g, ""),
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
// DECODE GMAIL BASE64URL DATA
// ============================================================

function decodeBase64Url(data) {
  if (!data) {
    return "";
  }

  const normalized = data
    .replace(/-/g, "+")
    .replace(/_/g, "/");

  const padded =
    normalized + "=".repeat((4 - normalized.length % 4) % 4);

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
// Prefer text/plain; fall back to text/html.
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

    const mimeType = (
      part.mimeType || ""
    ).toLowerCase();

    const data = part.body?.data;

    if (data) {
      const decoded = decodeBase64Url(data);

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

async function updateLastCheckedAt(env, integrationId) {
  const query = new URLSearchParams({
    id: `eq.${integrationId}`,
    integration_id: "eq.gmail"
  });

  await supabaseRequest(
    env,
    `user_integrations?${query.toString()}`,
    {
      method: "PATCH",
      headers: {
        Prefer: "return=minimal"
      },
      body: JSON.stringify({
        last_checked_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
    }
  );
}


// ============================================================
// JSON RESPONSE HELPER
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
