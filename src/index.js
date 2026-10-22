// ============================================================
// GMAIL INBOX COLLECTOR
// Cloudflare Worker + Supabase + Composio
//
// Features:
// 1. Select oldest unchecked connected Gmail account
// 2. Find unread emails
// 3. Save email details to Supabase
// 4. Avoid duplicate records
// 5. Mark successfully saved emails as read
// 6. Update last_checked_at
//
// No AI replies or drafts are generated.
// ============================================================

const WORKER_NAME = "gmail-inbox-collector";

const GMAIL_INTEGRATIONS_TABLE = "user_integrations";
const EMAILS_TABLE = "gmail_processed_messages";

const COMPOSIO_EXECUTE_URL =
  "https://backend.composio.dev/api/v3.1/tools/execute/proxy";

const GMAIL_API_BASE =
  "https://gmail.googleapis.com/gmail/v1";

const DEFAULT_MAX_EMAILS_PER_PAGE = 100;
const DEFAULT_MAX_PAGES_PER_RUN = 5;

// ============================================================
// MAIN REQUEST HANDLER
// ============================================================

export default {
  async fetch(request, env) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Content-Type": "application/json",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders,
      });
    }

    if (request.method !== "POST") {
      return jsonResponse(
        {
          success: false,
          worker: WORKER_NAME,
          error: "Use POST to run the Gmail collector.",
        },
        405,
        corsHeaders
      );
    }

    try {
      validateEnvironment(env);

      const integration = await getOldestGmailIntegration(env);

      if (!integration) {
        return jsonResponse(
          {
            success: true,
            worker: WORKER_NAME,
            message: "No connected Gmail integration needs checking.",
            stats: emptyStats(),
          },
          200,
          corsHeaders
        );
      }

      let stats;

      try {
        stats = await processGmailAccount(env, integration);
      } finally {
        // Update the selected account's check time even if processing fails.
        // This prevents one broken account from permanently blocking the queue.
        try {
          await updateLastCheckedAt(env, integration.id);
        } catch (updateError) {
          console.error(
            "Could not update last_checked_at:",
            updateError.message
          );
        }
      }

      return jsonResponse(
        {
          success: true,
          worker: WORKER_NAME,
          message: "Gmail inbox processing finished.",
          account_id: integration.id,
          application_id: integration.application_id,
          stats,
        },
        200,
        corsHeaders
      );
    } catch (error) {
      console.error("Gmail collector error:", error.message);

      return jsonResponse(
        {
          success: false,
          worker: WORKER_NAME,
          message: "Gmail processing encountered an error.",
          error: error.message,
        },
        500,
        corsHeaders
      );
    }
  },
};

// ============================================================
// RESPONSE HELPERS
// ============================================================

function jsonResponse(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers,
  });
}

function emptyStats() {
  return {
    emails_found: 0,
    emails_saved: 0,
    duplicates: 0,
    emails_marked_read: 0,
    email_errors: 0,
    pages_processed: 0,
  };
}

// ============================================================
// ENVIRONMENT VALIDATION
// ============================================================

function validateEnvironment(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "COMPOSIO_API_KEY",
  ];

  const missing = required.filter(
    (key) => !env[key] || !String(env[key]).trim()
  );

  if (missing.length > 0) {
    throw new Error(
      `Missing Cloudflare Worker secrets: ${missing.join(", ")}`
    );
  }
}

// ============================================================
// SUPABASE REST HELPER
// ============================================================

async function supabaseRequest(env, path, options = {}) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const headers = {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...options.headers,
  };

  const response = await fetch(
    `${baseUrl}/rest/v1/${path}`,
    {
      ...options,
      headers,
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
// SELECT OLDEST CONNECTED GMAIL INTEGRATION
// ============================================================

async function getOldestGmailIntegration(env) {
  const query = new URLSearchParams({
    select:
      "id,user_id,application_id,connection_id,integration_id,status,last_checked_at",
    integration_id: "eq.gmail",
    status: "eq.connected",
    order: "last_checked_at.asc.nullsfirst",
    limit: "1",
  });

  const rows = await supabaseRequest(
    env,
    `${GMAIL_INTEGRATIONS_TABLE}?${query.toString()}`,
    {
      method: "GET",
    }
  );

  if (!Array.isArray(rows) || rows.length === 0) {
    return null;
  }

  const integration = rows[0];

  if (!integration.connection_id) {
    throw new Error(
      `Gmail integration ${integration.id} has no connection_id.`
    );
  }

  return integration;
}

// ============================================================
// UPDATE LAST CHECKED TIME
// ============================================================

async function updateLastCheckedAt(env, integrationId) {
  const query = new URLSearchParams({
    id: `eq.${integrationId}`,
    integration_id: "eq.gmail",
  });

  const now = new Date().toISOString();

  await supabaseRequest(
    env,
    `${GMAIL_INTEGRATIONS_TABLE}?${query.toString()}`,
    {
      method: "PATCH",
      headers: {
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        last_checked_at: now,
        updated_at: now,
      }),
    }
  );
}

// ============================================================
// COMPOSIO PROXY EXECUTION
//
// IMPORTANT FIX:
// Composio's raw proxy endpoint expects parameter.type to be
// "query" or "header", NOT "string".
//
// The Gmail API path is passed as an endpoint relative to the
// Gmail API base URL.
// ============================================================

async function composioGmailRequest(
  env,
  connectedAccountId,
  endpoint,
  method = "GET",
  options = {}
) {
  const payload = {
    connected_account_id: connectedAccountId,
    endpoint,
    method: method.toUpperCase(),
  };

  // Correct Composio parameter format.
  // Do not send type: "string".
  if (
    Array.isArray(options.parameters) &&
    options.parameters.length > 0
  ) {
    payload.parameters = options.parameters.map((parameter) => ({
      name: parameter.name,
      value: String(parameter.value),
      type: parameter.in === "header" ? "header" : "query",
    }));
  }

  if (options.body !== undefined && options.body !== null) {
    payload.body = options.body;
  }

  const response = await fetch(COMPOSIO_EXECUTE_URL, {
    method: "POST",
    headers: {
      "x-api-key": env.COMPOSIO_API_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const responseText = await response.text();

  let result;

  try {
    result = responseText ? JSON.parse(responseText) : {};
  } catch {
    throw new Error(
      `Composio returned non-JSON HTTP ${response.status}: ${responseText}`
    );
  }

  if (!response.ok) {
    throw new Error(
      `Composio HTTP ${response.status}: ${responseText}`
    );
  }

  // Composio may return an outer successful HTTP response while
  // the proxied Gmail request itself has failed.
  const upstreamStatus =
    result.status ??
    result.response?.status ??
    result.data?.status;

  if (
    typeof upstreamStatus === "number" &&
    upstreamStatus >= 400
  ) {
    throw new Error(
      `Gmail API HTTP ${upstreamStatus}: ${JSON.stringify(result.data ?? result)}`
    );
  }

  if (result.error) {
    throw new Error(
      `Composio execution error: ${JSON.stringify(result.error)}`
    );
  }

  return result;
}

// ============================================================
// EXTRACT PROXIED RESPONSE DATA
// ============================================================

function extractResponseData(result) {
  if (!result || typeof result !== "object") {
    return {};
  }

  // Common Composio proxy response shape.
  if (result.data !== undefined) {
    return result.data;
  }

  if (result.response?.data !== undefined) {
    return result.response.data;
  }

  return result;
}

// ============================================================
// GMAIL GET HELPER
// ============================================================

async function gmailGet(
  env,
  connectionId,
  endpoint,
  queryParameters = []
) {
  const result = await composioGmailRequest(
    env,
    connectionId,
    endpoint,
    "GET",
    {
      parameters: queryParameters.map((parameter) => ({
        name: parameter.name,
        value: parameter.value,
        in: "query",
      })),
    }
  );

  return extractResponseData(result);
}

// ============================================================
// GMAIL POST HELPER
// ============================================================

async function gmailPost(
  env,
  connectionId,
  endpoint,
  body
) {
  const result = await composioGmailRequest(
    env,
    connectionId,
    endpoint,
    "POST",
    {
      body,
    }
  );

  return extractResponseData(result);
}

// ============================================================
// PROCESS ONE GMAIL ACCOUNT
// ============================================================

async function processGmailAccount(env, integration) {
  const stats = emptyStats();

  const connectionId = integration.connection_id;

  const maxEmailsPerPage = readPositiveInteger(
    env.MAX_EMAILS_PER_PAGE,
    DEFAULT_MAX_EMAILS_PER_PAGE
  );

  const maxPagesPerRun = readPositiveInteger(
    env.MAX_PAGES_PER_RUN,
    DEFAULT_MAX_PAGES_PER_RUN
  );

  let pageToken = null;

  for (
    let pageNumber = 0;
    pageNumber < maxPagesPerRun;
    pageNumber++
  ) {
    const parameters = [
      {
        name: "q",
        value: "is:unread newer_than:1d",
      },
      {
        name: "maxResults",
        value: String(maxEmailsPerPage),
      },
    ];

    if (pageToken) {
      parameters.push({
        name: "pageToken",
        value: pageToken,
      });
    }

    // List unread Gmail messages.
    const listData = await gmailGet(
      env,
      connectionId,
      `${GMAIL_API_BASE}/users/me/messages`,
      parameters
    );

    const messages = Array.isArray(listData.messages)
      ? listData.messages
      : [];

    stats.pages_processed++;

    if (messages.length === 0) {
      break;
    }

    for (const message of messages) {
      if (!message.id) {
        stats.email_errors++;
        continue;
      }

      stats.emails_found++;

      try {
        // Fetch complete message.
        const messageData = await gmailGet(
          env,
          connectionId,
          `${GMAIL_API_BASE}/users/me/messages/${encodeURIComponent(message.id)}`,
          [
            {
              name: "format",
              value: "full",
            },
          ]
        );

        const email = extractEmailDetails(messageData);

        // Save first. Only mark as read after a successful save
        // or confirmation that the email already exists.
        const saveResult = await saveEmailToSupabase(
          env,
          integration,
          message.id,
          email
        );

        if (saveResult === "inserted") {
          stats.emails_saved++;
        } else {
          stats.duplicates++;
        }

        // Mark as read after saving or confirming a duplicate.
        await gmailPost(
          env,
          connectionId,
          `${GMAIL_API_BASE}/users/me/messages/${encodeURIComponent(message.id)}/modify`,
          {
            removeLabelIds: ["UNREAD"],
          }
        );

        stats.emails_marked_read++;
      } catch (emailError) {
        stats.email_errors++;

        console.error(
          `Could not process Gmail message ${message.id}:`,
          emailError.message
        );
      }
    }

    pageToken = listData.nextPageToken || null;

    if (!pageToken) {
      break;
    }
  }

  return stats;
}

// ============================================================
// READ POSITIVE INTEGER FROM WORKER ENVIRONMENT
// ============================================================

function readPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

// ============================================================
// EXTRACT EMAIL HEADERS AND BODY
// ============================================================

function extractEmailDetails(message) {
  const headers = message.payload?.headers || [];

  const getHeader = (name) => {
    const found = headers.find(
      (header) =>
        String(header.name || "").toLowerCase() ===
        name.toLowerCase()
    );

    return found?.value || "";
  };

  const fromHeader = getHeader("From");
  const toHeader = getHeader("To");
  const subject = getHeader("Subject");
  const dateHeader = getHeader("Date");

  const sender = parseEmailAddress(fromHeader);
  const recipient = parseEmailAddress(toHeader);

  const body = extractMessageBody(message.payload);

  // email_content stores the subject plus the body because the
  // database schema uses email_content rather than subject.
  const emailContent = subject
    ? `Subject: ${subject}\n\n${body}`
    : body;

  const receivedAt = message.internalDate
    ? new Date(Number(message.internalDate)).toISOString()
    : parseEmailDate(dateHeader);

  return {
    sender_email: sender.email,
    sender_name: sender.name,
    recipient_email: recipient.email,
    email_content: emailContent,
    email_received_at: receivedAt,
    email_category: null,
    should_reply: null,
  };
}

// ============================================================
// PARSE EMAIL ADDRESS
// ============================================================

function parseEmailAddress(value) {
  if (!value) {
    return {
      name: null,
      email: null,
    };
  }

  const textValue = String(value).trim();

  const match = textValue.match(/^(.*?)\s*<([^<>]+)>$/);

  if (match) {
    return {
      name: cleanDisplayName(match[1]),
      email: match[2].trim().toLowerCase(),
    };
  }

  const emailMatch = textValue.match(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i
  );

  return {
    name: null,
    email: emailMatch ? emailMatch[0].toLowerCase() : null,
  };
}

function cleanDisplayName(value) {
  if (!value) {
    return null;
  }

  return String(value)
    .trim()
    .replace(/^["']|["']$/g, "") || null;
}

// ============================================================
// PARSE EMAIL DATE
// ============================================================

function parseEmailDate(value) {
  if (!value) {
    return new Date().toISOString();
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return new Date().toISOString();
  }

  return parsed.toISOString();
}

// ============================================================
// EXTRACT GMAIL MESSAGE BODY
// Supports text/plain and text/html MIME parts.
// ============================================================

function extractMessageBody(payload) {
  if (!payload) {
    return "";
  }

  const parts = [];

  collectBodyParts(payload, parts);

  const plainTextPart = parts.find(
    (part) => part.mimeType === "text/plain"
  );

  if (plainTextPart) {
    return decodeBase64Url(plainTextPart.body?.data || "");
  }

  const htmlPart = parts.find(
    (part) => part.mimeType === "text/html"
  );

  if (htmlPart) {
    return htmlToText(
      decodeBase64Url(htmlPart.body?.data || "")
    );
  }

  if (payload.body?.data) {
    const decoded = decodeBase64Url(payload.body.data);

    if (payload.mimeType === "text/html") {
      return htmlToText(decoded);
    }

    return decoded;
  }

  return "";
}

function collectBodyParts(part, output) {
  if (!part) {
    return;
  }

  if (
    part.body?.data &&
    (part.mimeType === "text/plain" ||
      part.mimeType === "text/html")
  ) {
    output.push(part);
  }

  if (Array.isArray(part.parts)) {
    for (const child of part.parts) {
      collectBodyParts(child, output);
    }
  }
}

// ============================================================
// DECODE GMAIL BASE64URL BODY
// ============================================================

function decodeBase64Url(value) {
  if (!value) {
    return "";
  }

  try {
    const base64 = String(value)
      .replace(/-/g, "+")
      .replace(/_/g, "/");

    const padded = base64.padEnd(
      Math.ceil(base64.length / 4) * 4,
      "="
    );

    const binary = atob(padded);
    const bytes = Uint8Array.from(
      binary,
      (character) => character.charCodeAt(0)
    );

    return new TextDecoder("utf-8", {
      fatal: false,
    }).decode(bytes);
  } catch (error) {
    console.error("Could not decode Gmail message body:", error.message);
    return "";
  }
}

// ============================================================
// BASIC HTML TO TEXT CONVERSION
// ============================================================

function htmlToText(html) {
  return String(html || "")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|tr|h[1-6])\s*>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

// ============================================================
// SAVE EMAIL TO SUPABASE
//
// Required unique constraint:
// UNIQUE (connected_account_id, gmail_message_id)
//
// On duplicate, do not create another record.
// ============================================================

async function saveEmailToSupabase(
  env,
  integration,
  gmailMessageId,
  email
) {
  const query = new URLSearchParams({
    on_conflict: "connected_account_id,gmail_message_id",
  });

  const record = {
    user_id: integration.user_id,
    application_id: integration.application_id || null,
    connected_account_id: integration.connection_id,
    gmail_message_id: gmailMessageId,
    sender_email: email.sender_email,
    sender_name: email.sender_name,
    recipient_email: email.recipient_email,
    email_content: email.email_content,
    email_received_at: email.email_received_at,
    email_category: email.email_category,
    should_reply: email.should_reply,

    // AI reply generation is not part of this worker.
    ai_draft_reply: null,
    gmail_draft_id: null,

    status: "pending",
    error_message: null,
    processed_at: null,
    updated_at: new Date().toISOString(),
  };

  const response = await fetch(
    `${env.SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/${EMAILS_TABLE}?${query.toString()}`,
    {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "resolution=ignore-duplicates,return=representation",
      },
      body: JSON.stringify(record),
    }
  );

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Could not save email to Supabase. HTTP ${response.status}: ${responseText}`
    );
  }

  let returnedRows = [];

  try {
    returnedRows = responseText ? JSON.parse(responseText) : [];
  } catch {
    returnedRows = [];
  }

  // With return=representation, an inserted record is returned.
  // An ignored duplicate usually returns an empty array.
  if (Array.isArray(returnedRows) && returnedRows.length > 0) {
    return "inserted";
  }

  return "duplicate";
    }
