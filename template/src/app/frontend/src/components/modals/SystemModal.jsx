import React, { useState, useRef } from 'react';

const SYSTEM_KINDS = ['Databricks', 'Netezza', 'Teradata', 'Oracle', 'Postgres', 'SQLServer', 'MySQL', 'other'];

// Helper to determine default max_rows based on system kind
const getDefaultMaxRows = (kind) => {
  // Databricks and Snowflake: unlimited (null)
  // All others: 1,000,000 default
  return ['Databricks', 'Snowflake'].includes(kind) ? null : 1000000;
};

export function SystemModal({ system, onSave, onClose }) {
  const [form, setForm] = useState(() => {
    const initialKind = system?.kind || "Databricks";
    return {
      name: system?.name || "New System",
      kind: initialKind,
      catalog: system?.catalog || "",
      host: system?.host || "",
      port: system?.port || 443,
      database: system?.database || "",
      user_secret_key: system?.user_secret_key || "",
      pass_secret_key: system?.pass_secret_key || "",
      jdbc_string: system?.jdbc_string || "",
      concurrency: system?.concurrency ?? -1,
      max_rows: system?.max_rows !== undefined ? system.max_rows : getDefaultMaxRows(initialKind),
      version: system?.version || 0
    };
  });
  
  const handleSave = async () => {
    await onSave(form);
  };
  
  const backdropRef = useRef(null);
  const mouseDownTarget = useRef(null);
  
  const handleMouseDown = (e) => {
    mouseDownTarget.current = e.target;
  };
  
  const handleMouseUp = (e) => {
    if (mouseDownTarget.current === backdropRef.current && e.target === backdropRef.current) {
      onClose();
    }
    mouseDownTarget.current = null;
  };
  
  const isDatabricks = form.kind === 'Databricks';
  const showDatabase = ['Postgres', 'SQLServer', 'MySQL', 'Netezza'].includes(form.kind);
  
  return (
    <div ref={backdropRef} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} className="fixed inset-0 bg-black/75 flex items-center justify-center z-50">
      <div onClick={(e)=>e.stopPropagation()} className="bg-charcoal-500 rounded-xl w-full max-w-lg shadow-2xl border border-charcoal-200">
        <div className="border-b border-charcoal-200 px-4 py-3 bg-charcoal-400">
          <h3 className="m-0 text-rust text-lg font-semibold">{system ? "Edit System" : "New System"}</h3>
        </div>
        <div className="p-4 max-h-[70vh] overflow-y-auto">
          {/* Name - Always shown */}
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Name</label>
            <input value={form.name} onChange={e=>setForm({...form, name:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
          </div>
          
          {/* Kind - Dropdown */}
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Type</label>
            <select value={form.kind} onChange={e=>{
              const newKind = e.target.value;
              setForm({
                ...form, 
                kind: newKind,
                // Update max_rows default when changing kind (only if not editing existing system with explicit value)
                max_rows: system?.max_rows !== undefined ? form.max_rows : getDefaultMaxRows(newKind)
              });
            }} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500">
              {SYSTEM_KINDS.map(k => <option key={k} value={k}>{k}</option>)}
            </select>
          </div>
          
          {/* Databricks: Only show catalog */}
          {isDatabricks && (
            <div className="mb-3">
              <label className="block mb-1 font-medium text-gray-400 text-sm">Catalog</label>
              <input value={form.catalog} onChange={e=>setForm({...form, catalog:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="e.g., main" />
            </div>
          )}
          
          {/* Non-Databricks: Show connection fields */}
          {!isDatabricks && (
            <>
              <div className="grid grid-cols-2 gap-3 mb-3">
                <div>
                  <label className="block mb-1 font-medium text-gray-400 text-sm">Host</label>
                  <input value={form.host} onChange={e=>setForm({...form, host:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
                </div>
                <div>
                  <label className="block mb-1 font-medium text-gray-400 text-sm">Port</label>
                  <input type="number" value={form.port} onChange={e=>setForm({...form, port:+e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
                </div>
              </div>
              
              {/* Database - Only for specific kinds */}
              {showDatabase && (
                <div className="mb-3">
                  <label className="block mb-1 font-medium text-gray-400 text-sm">Database</label>
                  <input value={form.database} onChange={e=>setForm({...form, database:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
                </div>
              )}
              
              <div className="mb-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">User Secret Key</label>
                <input value={form.user_secret_key} onChange={e=>setForm({...form, user_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="Databricks secret scope/key" />
              </div>
              
              <div className="mb-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">Pass Secret Key</label>
                <input value={form.pass_secret_key} onChange={e=>setForm({...form, pass_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="Databricks secret scope/key" />
              </div>
              
              <div className="mb-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">JDBC String (Optional)</label>
                <textarea value={form.jdbc_string} onChange={e=>setForm({...form, jdbc_string:e.target.value})} rows={3} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="Full JDBC connection string if needed" />
              </div>
              
              <div className="mb-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">Concurrency Limit</label>
                <input type="number" value={form.concurrency} onChange={e=>setForm({...form, concurrency:+e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="-1 for unlimited" />
                <p className="text-xs text-gray-500 mt-1">-1 = unlimited, 0 = disabled, positive = max concurrent connections</p>
              </div>
              
              <div className="mb-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">Max Rows per Query</label>
                <input type="number" value={form.max_rows ?? ""} onChange={e=>setForm({...form, max_rows: e.target.value ? +e.target.value : null})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="Default: 1,000,000" />
                <p className="text-xs text-gray-500 mt-1">Limits rows pulled during validation to protect system performance (empty = unlimited)</p>
              </div>
            </>
          )}
        </div>
        <div className="border-t border-charcoal-200 px-4 py-3 flex gap-2 justify-end bg-charcoal-400">
          <button onClick={onClose} className="px-3 py-2 bg-charcoal-700 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-600">Cancel</button>
          <button onClick={handleSave} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Save</button>
        </div>
      </div>
    </div>
  );
}

