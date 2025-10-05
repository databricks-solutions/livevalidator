const DEFAULT_USER = "user@company.com";

/**
 * Generic API call handler
 */
export async function apiCall(method, url, body) {
  const opts = { method, headers: {"Content-Type":"application/json"} };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    let detail = text;
    let action = null;
    let message = null;
    try {
      const parsed = JSON.parse(text);
      if (parsed.detail) detail = typeof parsed.detail === 'string' ? parsed.detail : JSON.stringify(parsed.detail);
      if (parsed.action) action = parsed.action;
      if (parsed.message) message = parsed.message;
    } catch {}
    
    // If this is a setup_required error, throw a special error
    if (action === "setup_required") {
      const err = new Error(message || detail);
      err.action = "setup_required";
      throw err;
    }
    
    throw new Error(`${res.status} ${res.statusText}: ${detail}`);
  }
  return res.json();
}

/**
 * Table operations
 */
export const tableService = {
  list: () => fetch("/api/tables").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/tables/${id}`),
  create: (data) => apiCall("POST", "/api/tables", { ...data, updated_by: DEFAULT_USER }),
  update: (id, data) => apiCall("PUT", `/api/tables/${id}`, { ...data, updated_by: DEFAULT_USER }),
  delete: (id) => apiCall("DELETE", `/api/tables/${id}`),
  bulkUpload: (srcSystemId, tgtSystemId, items) => 
    apiCall("POST", "/api/tables/bulk", { 
      src_system_id: srcSystemId, 
      tgt_system_id: tgtSystemId, 
      items, 
      updated_by: DEFAULT_USER 
    }),
};

/**
 * Query operations
 */
export const queryService = {
  list: () => fetch("/api/queries").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/queries/${id}`),
  create: (data) => apiCall("POST", "/api/queries", { ...data, updated_by: DEFAULT_USER }),
  update: (id, data) => apiCall("PUT", `/api/queries/${id}`, { ...data, updated_by: DEFAULT_USER }),
  delete: (id) => apiCall("DELETE", `/api/queries/${id}`),
  bulkUpload: (srcSystemId, tgtSystemId, items) => 
    apiCall("POST", "/api/queries/bulk", { 
      src_system_id: srcSystemId, 
      tgt_system_id: tgtSystemId, 
      items, 
      updated_by: DEFAULT_USER 
    }),
};

/**
 * Schedule operations
 */
export const scheduleService = {
  list: () => fetch("/api/schedules").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/schedules/${id}`),
  create: (data) => apiCall("POST", "/api/schedules", { ...data, updated_by: DEFAULT_USER }),
  update: (id, data) => apiCall("PUT", `/api/schedules/${id}`, { ...data, updated_by: DEFAULT_USER }),
  delete: (id) => apiCall("DELETE", `/api/schedules/${id}`),
};

/**
 * System operations
 */
export const systemService = {
  list: () => fetch("/api/systems").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/systems/${id}`),
  create: (data) => apiCall("POST", "/api/systems", { ...data, updated_by: DEFAULT_USER }),
  update: (id, data) => apiCall("PUT", `/api/systems/${id}`, { ...data, updated_by: DEFAULT_USER }),
  delete: (id) => apiCall("DELETE", `/api/systems/${id}`),
};

/**
 * Binding operations
 */
export const bindingService = {
  create: (scheduleId, entityType, entityId) => 
    apiCall("POST", "/api/bindings", { schedule_id: scheduleId, entity_type: entityType, entity_id: entityId }),
};

/**
 * Trigger operations
 */
export const triggerService = {
  create: (entityType, entityId) => 
    apiCall("POST", "/api/triggers", { entity_type: entityType, entity_id: entityId, requested_by: DEFAULT_USER }),
};

