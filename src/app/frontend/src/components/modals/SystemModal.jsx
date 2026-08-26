import React, { useState, useRef, useEffect, useCallback } from 'react';
import { apiCall } from '../../services/api';

const SYSTEM_KINDS = ['Databricks', 'Netezza', 'Teradata', 'Oracle', 'Postgres', 'Redshift', 'SQLServer', 'Synapse', 'MySQL', 'other'];

// Helper to determine default max_rows based on system kind
const getDefaultMaxRows = (kind) => {
  // Databricks and Snowflake: unlimited (null)
  // All others: 1,000,000 default
  return ['Databricks', 'Snowflake'].includes(kind) ? null : 1000000;
};

// Helper to determine default port based on system kind
const getDefaultPort = (kind) => {
  switch (kind) {
    case 'Oracle': return 1521;
    case 'Postgres': return 5432;
    case 'MySQL': return 3306;
    case 'SQLServer': case 'Synapse': return 1433;
    case 'Netezza': return 5480;
    case 'Redshift': return 5439;
    case 'Teradata': return 443;
    default: return 443;
  }
};

// Default JDBC driver classes by system type
const getDefaultDriver = (kind) => {
  switch (kind) {
    case 'Oracle': return 'oracle.jdbc.OracleDriver';
    case 'Postgres': return 'org.postgresql.Driver';
    case 'MySQL': return 'com.mysql.cj.jdbc.Driver';
    case 'SQLServer': case 'Synapse': return 'com.microsoft.sqlserver.jdbc.SQLServerDriver';
    case 'Netezza': return 'org.netezza.Driver';
    case 'Redshift': return 'com.amazon.redshift.jdbc42.Driver';
    case 'Teradata': return 'com.teradata.jdbc.TeraDriver';
    default: return '';
  }
};

// Generate JDBC preview string based on system type
const getJdbcPreview = (kind, host, port, database) => {
  const h = host || '<host>';
  const p = port || '<port>';
  const d = database || '<database>';
  
  switch (kind) {
    case 'Oracle':
      return `jdbc:oracle:thin:@//${h}:${p}/${d}`;
    case 'SQLServer':
      return `jdbc:sqlserver://${h}:${p};databaseName=${d};encrypt=true;trustServerCertificate=true`;
    case 'Synapse':
      return `jdbc:sqlserver://${h}:${p};database=${d};encrypt=true;trustServerCertificate=false`;
    case 'Teradata':
      return `jdbc:teradata://${h}`;
    case 'Redshift':
      return `jdbc:redshift://${h}:${p}/${d}`;
    default:
      return `jdbc:${kind.toLowerCase()}://${h}:${p}/${d}`;
  }
};

function ErrorPopover({ error }) {
  const [open, setOpen] = useState(false);
  if (!error) return null;
  return (
    <span className="relative">
      <button onClick={() => setOpen(!open)} className="text-red-400 hover:text-red-300 underline cursor-pointer text-xs bg-transparent border-0 p-0">
        — Error details
      </button>
      {open && (
        <div className="absolute bottom-full left-0 mb-1 w-80 p-3 bg-charcoal-600 border border-red-500/40 rounded-lg shadow-xl z-50 text-xs text-red-200 whitespace-pre-wrap break-words max-h-40 overflow-y-auto">
          {error}
        </div>
      )}
    </span>
  );
}

export function SystemModal({ system, onSave, onClose }) {
  const [form, setForm] = useState(() => {
    const initialKind = system?.kind || "Databricks";
    return {
      name: system?.name || "New System",
      kind: initialKind,
      catalog: system?.catalog || "",
      host: system?.host || "",
      port: system?.port ?? getDefaultPort(initialKind),
      database: system?.database || "",
      secret_scope: system?.secret_scope || "livevalidator",
      user_secret_key: system?.user_secret_key || "",
      pass_secret_key: system?.pass_secret_key || "",
      jdbc_string: system?.jdbc_string || "",
      driver_connector: system?.driver_connector || getDefaultDriver(initialKind),
      compute_mode: system?.compute_mode || "prefer_serverless",
      jdbc_method: system?.jdbc_method || "uc_jdbc_connection",
      uc_connection_name: system?.uc_connection_name || "",
      concurrency: system?.concurrency ?? -1,
      max_rows: system?.max_rows !== undefined ? system.max_rows : getDefaultMaxRows(initialKind),
      options: (typeof system?.options === 'string' ? JSON.parse(system.options) : system?.options) || {jdbc: {}},
      version: system?.version || 0
    };
  });
  
  const savedForm = useRef(form);
  useEffect(() => {
    if (system?.version !== undefined) {
      setForm(f => {
        const updated = { ...f, version: system.version };
        savedForm.current = updated;
        return updated;
      });
    }
  }, [system?.version]);

  const hasUnsavedChanges = JSON.stringify({ ...form, version: 0 }) !== JSON.stringify({ ...savedForm.current, version: 0 });

  // --- Test Connection ---
  // Each entry: { compute, run_id, run_url, state: "RUNNING"|"SUCCESS"|"FAILED", error? }
  const [tests, setTests] = useState([]);
  const [testLoading, setTestLoading] = useState(false);
  const [testError, setTestError] = useState(null);
  const pollRef = useRef(null);

  const pollStatuses = useCallback(async (items) => {
    const pending = items.filter(t => t.state === 'RUNNING');
    if (!pending.length) return;

    const updated = await Promise.all(items.map(async (t) => {
      if (t.state !== 'RUNNING') return t;
      try {
        const res = await apiCall('GET', `/api/systems/${system.id}/test/${t.run_id}/status`);
        return { ...t, state: res.state, error: res.error };
      } catch {
        return t;
      }
    }));
    setTests(updated);

    if (updated.some(t => t.state === 'RUNNING')) {
      pollRef.current = setTimeout(() => pollStatuses(updated), 3000);
    }
  }, [system?.id]);

  const handleTestConnection = async () => {
    setTestError(null);
    setTestLoading(true);
    if (pollRef.current) clearTimeout(pollRef.current);
    try {
      const res = await apiCall('POST', `/api/systems/${system.id}/test`);
      const items = res.tests.map(t => ({ ...t, state: 'RUNNING' }));
      setTests(items);
      pollRef.current = setTimeout(() => pollStatuses(items), 3000);
    } catch (e) {
      setTestError(e.message);
      setTests([]);
    } finally {
      setTestLoading(false);
    }
  };

  useEffect(() => {
    return () => { if (pollRef.current) clearTimeout(pollRef.current); };
  }, []);

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
  const isOracle = form.kind === 'Oracle';
  const isOther = form.kind === 'other';
  const isNetezza = form.kind === 'Netezza';
  const isTeradata = form.kind === 'Teradata';
  const needsManualDriver = isNetezza || isOther;
  const driverNotInDBR = ['Netezza', 'Teradata', 'Oracle', 'Redshift'].includes(form.kind);
  const showDatabase = ['Postgres', 'Redshift', 'SQLServer', 'Synapse', 'MySQL', 'Netezza', 'Oracle'].includes(form.kind);
  const showHostPort = !isDatabricks && !isOther;
  const isJdbc = !isDatabricks;
  const isDirectJdbc = isJdbc && form.jdbc_method === 'direct';
  const isUcManaged = isJdbc && (form.jdbc_method === 'uc_jdbc_connection' || form.jdbc_method === 'uc_connection');
  const showDirectFields = isJdbc && !isUcManaged;
  const showJdbcOptions = isJdbc && form.jdbc_method !== 'uc_connection';
  const showTeradataCredentials = isTeradata;
  const teradataMissingCreds = isTeradata && (!form.host || !form.secret_scope || !form.user_secret_key || !form.pass_secret_key);

  const COMPUTE_MODES = [
    { value: 'require_serverless', label: 'Require Serverless' },
    { value: 'prefer_serverless', label: 'Prefer Serverless' },
    { value: 'classic', label: 'Classic' },
  ];

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
                port: system?.port !== undefined ? form.port : getDefaultPort(newKind),
                max_rows: system?.max_rows !== undefined ? form.max_rows : getDefaultMaxRows(newKind),
                driver_connector: system?.driver_connector ? form.driver_connector : getDefaultDriver(newKind)
              });
            }} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500">
              {SYSTEM_KINDS.map(k => <option key={k} value={k}>{k === 'other' ? 'Other JDBC' : k}</option>)}
            </select>
            {needsManualDriver && (
              <div className="mt-2 px-3 py-2 bg-gradient-to-r from-amber-950/40 to-transparent border-l-2 border-amber-500 rounded-r text-xs text-amber-100/90">
                <span className="font-semibold text-amber-300">Driver Required</span> — {isNetezza ? 'Netezza' : 'This system type'} requires a JDBC driver JAR to be installed on the validation job's compute cluster.
              </div>
            )}
          </div>
          
          {/* Compute Mode */}
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Compute Mode</label>
            <div className="flex rounded-md overflow-hidden border border-charcoal-200">
              {COMPUTE_MODES.map(({ value, label }) => {
                const disabled = isDirectJdbc && value !== 'classic';
                const active = form.compute_mode === value;
                return (
                  <button
                    key={value}
                    type="button"
                    disabled={disabled}
                    onClick={() => setForm({ ...form, compute_mode: value })}
                    className={`flex-1 px-3 py-1.5 text-xs font-medium transition-colors ${
                      active
                        ? 'bg-purple-600 text-white'
                        : disabled
                        ? 'bg-charcoal-700 text-gray-600 cursor-not-allowed'
                        : 'bg-charcoal-400 text-gray-300 hover:bg-charcoal-300 cursor-pointer'
                    }`}
                    title={disabled ? 'Direct JDBC requires classic compute' : ''}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
            {isDirectJdbc && (
              <p className="text-xs text-amber-400 mt-1">Direct JDBC only supports classic compute</p>
            )}
          </div>

          {/* JDBC Method - only for non-Databricks */}
          {isJdbc && (
            <div className="mb-3">
              <label className="block mb-1 font-medium text-gray-400 text-sm">JDBC Method</label>
              <select
                value={form.jdbc_method}
                onChange={e => {
                  const newMethod = e.target.value;
                  const updates = { jdbc_method: newMethod };
                  if (newMethod === 'direct') updates.compute_mode = 'classic';
                  setForm({ ...form, ...updates });
                }}
                className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500"
              >
                <option value="direct"
                  disabled={form.compute_mode === 'require_serverless'}
                  title={form.compute_mode === 'require_serverless' ? 'Not available with Require Serverless' : ''}
                >
                  Direct JDBC
                </option>
                <option value="uc_jdbc_connection">UC JDBC Connection</option>
                <option value="uc_connection">UC Connection</option>
              </select>
              <p className="text-xs text-gray-500 mt-1">
                {form.jdbc_method === 'direct' && 'Spark JDBC with direct connection string (classic compute only)'}
                {form.jdbc_method === 'uc_jdbc_connection' && 'Spark JDBC via Unity Catalog managed connection (classic + serverless)'}
                {form.jdbc_method === 'uc_connection' && 'Query federation via Unity Catalog connection (classic + serverless, no partitioned reads)'}
              </p>
            </div>
          )}

          {/* UC Connection Name */}
          {isUcManaged && (
            <div className="mb-3">
              <label className="block mb-1 font-medium text-gray-400 text-sm">UC Connection Name</label>
              <input
                value={form.uc_connection_name}
                onChange={e => setForm({ ...form, uc_connection_name: e.target.value })}
                className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500"
                placeholder="e.g., my_oracle_connection"
              />
              <p className="text-xs text-gray-500 mt-1">
                {form.jdbc_method === 'uc_jdbc_connection'
                  ? <span>JDBC connection name created via <a href="https://docs.databricks.com/aws/en/connect/jdbc-connection#step-2-create-a-jdbc-connection" target="_blank" rel="noopener noreferrer" className="text-purple-400 hover:text-purple-300 underline">CREATE CONNECTION (JDBC)</a></span>
                  : <span>Query federation connection name created via <a href="https://docs.databricks.com/aws/en/query-federation/database-federation#create-a-connection" target="_blank" rel="noopener noreferrer" className="text-purple-400 hover:text-purple-300 underline">CREATE CONNECTION</a></span>
                }
              </p>
            </div>
          )}

          {/* Teradata: always needs host and secrets for teradatasql column detection */}
          {showTeradataCredentials && (
            <div className="mb-3">
              {isUcManaged && (
                <div className="px-3 py-2 mb-3 bg-gradient-to-r from-amber-950/40 to-transparent border-l-2 border-amber-500 rounded-r text-xs text-amber-100/90">
                  <span className="font-semibold text-amber-300">Teradata Credentials Required</span> — The <code className="text-amber-200">teradatasql</code> Python package is used for column type detection and requires direct host access and credentials, even when using a UC connection for data reads.
                </div>
              )}
              <label className="block mb-1 font-medium text-gray-400 text-sm">Host</label>
              <input value={form.host} onChange={e=>setForm({...form, host:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
              <div className="mt-3">
                <label className="block mb-1 font-medium text-gray-400 text-sm">Secret Scope</label>
                <input value={form.secret_scope} onChange={e=>setForm({...form, secret_scope:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="livevalidator" />
              </div>
              <div className="grid grid-cols-2 gap-3 mt-3">
                <div>
                  <label className="block mb-1 font-medium text-gray-400 text-sm">User Secret Key</label>
                  <input value={form.user_secret_key} onChange={e=>setForm({...form, user_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="username-key" />
                </div>
                <div>
                  <label className="block mb-1 font-medium text-gray-400 text-sm">Pass Secret Key</label>
                  <input value={form.pass_secret_key} onChange={e=>setForm({...form, pass_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="password-key" />
                </div>
              </div>
            </div>
          )}

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
              {/* Direct JDBC fields — hidden for UC managed methods */}
              {showDirectFields && (
                <>
                  {/* "other" type: JDBC String is required and shown first */}
                  {isOther && (
                    <div className="mb-3">
                      <label className="block mb-1 font-medium text-gray-400 text-sm">JDBC String</label>
                      <textarea value={form.jdbc_string} onChange={e=>setForm({...form, jdbc_string:e.target.value})} rows={3} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="jdbc:snowflake://account.snowflakecomputing.com/?db=mydb&warehouse=compute_wh" />
                      <p className="text-xs text-gray-500 mt-1">Full JDBC connection string (required)</p>
                    </div>
                  )}
                  
                  {/* Host/port/database — skip host for Teradata (shown in Teradata credentials block above) */}
                  {showHostPort && !isTeradata && (
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
                  )}
                  {showDatabase && (
                    <div className="mb-3">
                      <label className="block mb-1 font-medium text-gray-400 text-sm">{isOracle ? 'Service Name' : 'Database'}</label>
                      <input value={form.database} onChange={e=>setForm({...form, database:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder={isOracle ? 'e.g., ORCL' : ''} />
                    </div>
                  )}

                  {/* Scope/user/pass — skip for Teradata (shown in Teradata credentials block above) */}
                  {!isTeradata && (
                    <>
                      <div className="mb-3">
                        <label className="block mb-1 font-medium text-gray-400 text-sm">Secret Scope</label>
                        <input value={form.secret_scope} onChange={e=>setForm({...form, secret_scope:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="livevalidator" />
                        <p className="text-xs text-gray-500 mt-1">Databricks secret scope name (default: livevalidator)</p>
                      </div>
                      <div className="grid grid-cols-2 gap-3 mb-3">
                        <div>
                          <label className="block mb-1 font-medium text-gray-400 text-sm">User Secret Key</label>
                          <input value={form.user_secret_key} onChange={e=>setForm({...form, user_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="username-key" />
                        </div>
                        <div>
                          <label className="block mb-1 font-medium text-gray-400 text-sm">Pass Secret Key</label>
                          <input value={form.pass_secret_key} onChange={e=>setForm({...form, pass_secret_key:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="password-key" />
                        </div>
                      </div>
                    </>
                  )}
                </>
              )}
              
              {/* JDBC Options — shown for direct + uc_jdbc_connection, hidden for uc_connection */}
              {showJdbcOptions && (
                <div className="mb-3">
                  <label className="block mb-1 font-medium text-gray-400 text-sm">JDBC Options</label>
                  <div className="space-y-2">
                    {Object.entries(form.options.jdbc || {}).map(([key, val], idx) => (
                      <div key={idx} className="flex gap-2">
                        <input value={key} onChange={e => {
                          const newJdbc = {...(form.options.jdbc || {})};
                          const oldVal = newJdbc[key];
                          delete newJdbc[key];
                          newJdbc[e.target.value] = oldVal;
                          setForm({...form, options: {...form.options, jdbc: newJdbc}});
                        }} placeholder="key" className="flex-1 px-2 py-1 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 text-sm focus:outline-none focus:ring-2 focus:ring-purple-500" />
                        <input value={val} onChange={e => setForm({...form, options: {...form.options, jdbc: {...(form.options.jdbc || {}), [key]: e.target.value}}})} placeholder="value" className="flex-1 px-2 py-1 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 text-sm focus:outline-none focus:ring-2 focus:ring-purple-500" />
                        <button type="button" onClick={() => {
                          const newJdbc = {...(form.options.jdbc || {})};
                          delete newJdbc[key];
                          setForm({...form, options: {...form.options, jdbc: newJdbc}});
                        }} className="px-2 py-1 text-red-400 hover:text-red-300 text-sm">✕</button>
                      </div>
                    ))}
                    <button type="button" onClick={() => setForm({...form, options: {...form.options, jdbc: {...(form.options.jdbc || {}), '': ''}}})} className="text-xs text-purple-400 hover:text-purple-300">+ Add Option</button>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">Key-value pairs passed to Spark JDBC (e.g., sessionInitStatement, fetchsize)</p>
                </div>
              )}

              {/* JDBC String override + Driver — only for direct */}
              {showDirectFields && (
                <>
                  {!isOther && (
                    <div className="mb-3">
                      <label className="block mb-1 font-medium text-gray-400 text-sm">JDBC String (Optional)</label>
                      <textarea value={form.jdbc_string} onChange={e=>setForm({...form, jdbc_string:e.target.value})} rows={2} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder={getJdbcPreview(form.kind, form.host, form.port, form.database)} />
                      <p className="text-xs text-gray-500 mt-1">Leave empty to auto-generate from fields above</p>
                    </div>
                  )}
                  
                  <div className="mb-3">
                    <label className="block mb-1 font-medium text-gray-400 text-sm">Driver</label>
                    <input value={form.driver_connector} onChange={e=>setForm({...form, driver_connector:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="e.g., net.snowflake.client.jdbc.SnowflakeDriver" />
                    {driverNotInDBR && (
                      <p className="text-xs text-gray-500 mt-1">
                        Driver not included in Databricks Runtime. Add as a library dep in run_validation job or compute using <code className="text-gray-400">databricks.yml</code> and whitelist in <a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist" target="_blank" rel="noopener noreferrer" className="text-purple-400 hover:text-purple-300 underline">Unity Catalog allowlist</a>.
                      </p>
                    )}
                  </div>
                </>
              )}
              
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
        {/* Test Connection Results */}
        {(tests.length > 0 || testError) && (
          <div className="px-4 pb-2">
            {testError && <p className="text-xs text-red-400">{testError}</p>}
            {tests.map(t => (
              <div key={t.run_id} className="flex items-center gap-2 text-xs py-1">
                {t.state === 'RUNNING' && <span className="inline-block w-3 h-3 border-2 border-purple-400 border-t-transparent rounded-full animate-spin" />}
                {t.state === 'SUCCESS' && <span className="text-green-400">&#10003;</span>}
                {t.state === 'FAILED' && <span className="text-red-400">&#10007;</span>}
                <span className="text-gray-300 capitalize">{t.compute}:</span>
                <span className={t.state === 'SUCCESS' ? 'text-green-400' : t.state === 'FAILED' ? 'text-red-400' : 'text-gray-400'}>
                  {t.state === 'RUNNING' ? 'Running...' : t.state === 'SUCCESS' ? 'OK' : 'Failed'}
                </span>
                <ErrorPopover error={t.error} />
                <a href={t.run_url} target="_blank" rel="noopener noreferrer" className="text-purple-400 hover:text-purple-300 underline ml-auto">View Run</a>
              </div>
            ))}
          </div>
        )}
        <div className="border-t border-charcoal-200 px-4 py-3 flex gap-2 justify-end bg-charcoal-400">
          <button
            onClick={handleTestConnection}
            disabled={!system?.id || hasUnsavedChanges || testLoading || tests.some(t => t.state === 'RUNNING')}
            title={!system?.id ? 'Apply changes first to test connectivity' : hasUnsavedChanges ? 'Apply changes before testing' : ''}
            className="px-3 py-2 bg-orange-600 text-white border-0 rounded-md cursor-pointer hover:bg-orange-500 disabled:opacity-50 disabled:cursor-not-allowed mr-auto"
          >
            {testLoading ? 'Launching...' : 'Test Connection'}
          </button>
          <button onClick={onClose} className="px-3 py-2 bg-charcoal-700 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-600">Cancel</button>
          <button
            disabled={teradataMissingCreds}
            title={teradataMissingCreds ? 'Teradata requires host and secret credentials' : ''}
            onClick={() => onSave(form, { close: false })}
            className={`px-3 py-2 border-0 rounded-md cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed ${
              hasUnsavedChanges ? 'bg-orange-600 text-white hover:bg-orange-500' : 'bg-charcoal-700 text-gray-200 border border-charcoal-200 hover:bg-charcoal-600'
            }`}
          >Apply</button>
          <button disabled={teradataMissingCreds} title={teradataMissingCreds ? 'Teradata requires host and secret credentials' : ''} onClick={() => onSave(form, { close: true })} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 disabled:opacity-50 disabled:cursor-not-allowed">Save</button>
        </div>
      </div>
    </div>
  );
}

