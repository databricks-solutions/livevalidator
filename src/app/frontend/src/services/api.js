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
    
    // Check for Databricks VPN error
    if (res.status === 403 && (
      text.includes("Public access is not allowed for workspace") 
    )) {
      throw new Error("403: Not able to access Databricks workspace, enable VPN if applicable.");
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
  create: (data) => apiCall("POST", "/api/tables", data),
  update: (id, data) => apiCall("PUT", `/api/tables/${id}`, data),
  delete: (id) => apiCall("DELETE", `/api/tables/${id}`),
  bulkUpload: (srcSystemId, tgtSystemId, items) => 
    apiCall("POST", "/api/tables/bulk", { 
      src_system_id: srcSystemId, 
      tgt_system_id: tgtSystemId, 
      items
    }),
};

/**
 * Query operations
 */
export const queryService = {
  list: () => fetch("/api/queries").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/queries/${id}`),
  create: (data) => apiCall("POST", "/api/queries", data),
  update: (id, data) => apiCall("PUT", `/api/queries/${id}`, data),
  delete: (id) => apiCall("DELETE", `/api/queries/${id}`),
  bulkUpload: (srcSystemId, tgtSystemId, items) => 
    apiCall("POST", "/api/queries/bulk", { 
      src_system_id: srcSystemId, 
      tgt_system_id: tgtSystemId, 
      items
    }),
};

/**
 * Schedule operations
 */
export const scheduleService = {
  list: () => fetch("/api/schedules").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/schedules/${id}`),
  create: (data) => apiCall("POST", "/api/schedules", data),
  update: (id, data) => apiCall("PUT", `/api/schedules/${id}`, data),
  delete: (id) => apiCall("DELETE", `/api/schedules/${id}`),
};

/**
 * System operations
 */
export const systemService = {
  list: () => fetch("/api/systems").then(r => r.json()),
  get: (id) => apiCall("GET", `/api/systems/${id}`),
  create: (data) => apiCall("POST", "/api/systems", data),
  update: (id, data) => apiCall("PUT", `/api/systems/${id}`, data),
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
  list: (status) => {
    const url = status ? `/api/triggers?status=${status}` : "/api/triggers";
    return fetch(url).then(r => r.json());
  },
  create: (entityType, entityId) => 
    apiCall("POST", "/api/triggers", { entity_type: entityType, entity_id: entityId }),
  cancel: (id) => apiCall("DELETE", `/api/triggers/${id}`),
  queueStatus: () => fetch("/api/queue-status").then(r => r.json()),
};

/**
 * Validation history operations
 */
export const validationService = {
  list: (params = {}) => {
    const queryParams = new URLSearchParams();
    if (params.entity_type) queryParams.append('entity_type', params.entity_type);
    if (params.entity_id) queryParams.append('entity_id', params.entity_id);
    if (params.status) queryParams.append('status', params.status);
    if (params.schedule_id) queryParams.append('schedule_id', params.schedule_id);
    if (params.limit) queryParams.append('limit', params.limit);
    if (params.offset) queryParams.append('offset', params.offset);
    const url = `/api/validation-history${queryParams.toString() ? '?' + queryParams.toString() : ''}`;
    return fetch(url).then(r => r.json());
  },
  get: (id) => apiCall("GET", `/api/validation-history/${id}`),
  getLatest: (entityType, entityId) => 
    fetch(`/api/validation-history/entity/${entityType}/${entityId}/latest`).then(r => r.json()),
  deleteMultiple: (ids) => apiCall("DELETE", "/api/validation-history", { ids }),
};

/**
 * Tag operations
 */
export const tagService = {
  list: () => fetch("/api/tags").then(r => r.json()),
  create: (name) => apiCall("POST", "/api/tags", { name }),
  getEntityTags: (entityType, entityId) => 
    fetch(`/api/tags/entity/${entityType}/${entityId}`).then(r => r.json()),
  setEntityTags: (entityType, entityId, tags) => 
    apiCall("POST", `/api/tags/entity/${entityType}/${entityId}`, { tags }),
  bulkAdd: (entityType, entityIds, tags) => 
    apiCall("POST", "/api/tags/entity/bulk-add", { entity_type: entityType, entity_ids: entityIds, tags }),
  bulkRemove: (entityType, entityIds, tags) => 
    apiCall("POST", "/api/tags/entity/bulk-remove", { entity_type: entityType, entity_ids: entityIds, tags }),
};

/**
 * Type transformation operations
 */
export const typeTransformationService = {
  list: () => fetch("/api/type-transformations").then(r => r.json()),
  get: (systemAId, systemBId) => 
    apiCall("GET", `/api/type-transformations/${systemAId}/${systemBId}`),
  create: (data) => 
    apiCall("POST", "/api/type-transformations", data),
  update: (systemAId, systemBId, data) => 
    apiCall("PUT", `/api/type-transformations/${systemAId}/${systemBId}`, data),
  delete: (systemAId, systemBId) => 
    apiCall("DELETE", `/api/type-transformations/${systemAId}/${systemBId}`),
  getDefault: (systemKind) => 
    fetch(`/api/type-transformations/default/${systemKind}`).then(r => r.json()),
  validateCode: (code) => 
    apiCall("POST", "/api/validate-python", { code }),
};

/**
 * User role operations (admin only)
 */
export const userRoleService = {
  listUsers: () => apiCall("GET", "/api/admin/users"),
  setUserRole: (userEmail, role) => apiCall("PUT", `/api/admin/users/${userEmail}/role?role=${role}`),
  deleteUserRole: (userEmail) => apiCall("DELETE", `/api/admin/users/${userEmail}/role`),
};

/**
 * Admin configuration operations
 */
export const adminService = {
  getConfig: () => apiCall("GET", "/api/admin/config"),
  updateConfig: (key, value) => apiCall("PUT", `/api/admin/config/${key}?value=${encodeURIComponent(value)}`),
};

/**
 * Current user operations
 */
export const currentUserService = {
  get: () => fetch("/api/current_user").then(r => r.json()),
};

/**
 * Unified API object
 */
export const API = {
  getTimezones: () => fetch("/api/timezones").then(r => r.json()),
  initializeDatabase: () => apiCall("POST", "/api/initialize-database"),
  resetDatabase: () => apiCall("POST", "/api/reset-database"),
  currentUser: currentUserService,
  userRoles: userRoleService,
  admin: adminService,
};
