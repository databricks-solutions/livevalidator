import React from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { useCurrentUser, canManageSystems } from '../context/UserContext';

export function SystemsView({ 
  data, 
  loading, 
  error, 
  onEdit, 
  onDelete, 
  onClearError 
}) {
  const currentUser = useCurrentUser();
  const canAdd = canManageSystems(currentUser?.role);

  return (
    <>
      {error && error.action !== "setup_required" && error.action !== "credentials_required" && <ErrorBox message={error.message} onClose={onClearError} />}
      <h2 className="text-2xl font-semibold text-rust-light mb-4">Systems</h2>
      <button 
        onClick={() => onEdit({})} 
        disabled={!canAdd}
        title={!canAdd ? "Only CAN_MANAGE users can add systems" : "Add a new system"}
        className="mb-3 px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        Add System
      </button>
      {loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {data.map(row => {
            const isDatabricks = row.kind === 'Databricks';
            const isOracle = row.kind === 'Oracle';
            const showDatabase = ['Postgres', 'SQLServer', 'Synapse', 'MySQL', 'Netezza', 'Oracle'].includes(row.kind);
            return (
              <div key={row.id} className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-lg font-semibold text-rust-light">{row.name}</h3>
                  <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                    row.compute_mode === 'require_serverless' ? 'bg-green-900/50 text-green-300 border border-green-700' :
                    row.compute_mode === 'prefer_serverless' ? 'bg-blue-900/50 text-blue-300 border border-blue-700' :
                    'bg-gray-800 text-gray-400 border border-gray-600'
                  }`}>
                    {row.compute_mode === 'require_serverless' ? 'Serverless' :
                     row.compute_mode === 'prefer_serverless' ? 'Prefer Serverless' : 'Classic'}
                  </span>
                </div>
                <p className="text-gray-400 text-sm mb-1"><strong>Type:</strong> {row.kind}</p>
                
                {isDatabricks ? (
                  /* Databricks: Only show catalog */
                  <p className="text-gray-400 text-sm mb-3"><strong>Catalog:</strong> {row.catalog || <span className="text-gray-600">-</span>}</p>
                ) : (
                  /* Other systems: Show connection details */
                  <>
                    <p className="text-gray-400 text-sm mb-1"><strong>JDBC Method:</strong> {
                      row.jdbc_method === 'direct' ? 'Direct' :
                      row.jdbc_method === 'uc_jdbc_connection' ? 'UC JDBC Connection' : 'UC Connection'
                    }</p>
                    {row.uc_connection_name && <p className="text-gray-400 text-sm mb-1"><strong>UC Connection:</strong> {row.uc_connection_name}</p>}
                    {row.host && <p className="text-gray-400 text-sm mb-1"><strong>Host:</strong> {row.host}</p>}
                    {row.port && <p className="text-gray-400 text-sm mb-1"><strong>Port:</strong> {row.port}</p>}
                    {showDatabase && row.database && <p className="text-gray-400 text-sm mb-1"><strong>{isOracle ? 'Service Name' : 'Database'}:</strong> {row.database}</p>}
                    {row.user_secret_key && <p className="text-gray-400 text-sm mb-1"><strong>User Secret:</strong> {row.user_secret_key}</p>}
                    {row.jdbc_string && <p className="text-gray-400 text-sm mb-1 font-mono text-xs break-all"><strong>JDBC:</strong> {row.jdbc_string.substring(0, 50)}...</p>}
                    <p className="text-gray-400 text-sm mb-1">
                      <strong>Concurrency:</strong> {row.concurrency === -1 ? 'Unlimited' : row.concurrency === 0 ? 'Disabled' : row.concurrency}
                    </p>
                    {row.max_rows !== null && row.max_rows !== undefined && (
                      <p className="text-gray-400 text-sm mb-1">
                        <strong>Max Rows:</strong> {row.max_rows.toLocaleString()}
                      </p>
                    )}
                    {(() => {
                      const opts = typeof row.options === 'string' ? JSON.parse(row.options) : row.options;
                      return opts?.jdbc && Object.keys(opts.jdbc).length > 0 && (
                        <p className="text-gray-400 text-sm mb-1">
                          <strong>JDBC Options:</strong> {Object.keys(opts.jdbc).length}
                        </p>
                      );
                    })()}
                  </>
                )}
                
                <div className="flex gap-2 mt-3">
                  <button onClick={() => onEdit(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500">Edit</button>
                  <button onClick={() => onDelete('systems', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500">Del</button>
                </div>
              </div>
            );
          })}
        </div>
      )}
      <table className="mt-6 text-xs text-gray-500">
        <tbody>
          <tr><td className="pr-2 py-0.5 align-middle"><span className="inline-block px-1.5 py-px rounded-full bg-green-900/50 text-green-300 border border-green-700 font-medium">Serverless</span></td><td className="py-0.5">Must run on serverless compute. Incompatible with Classic systems.</td></tr>
          <tr><td className="pr-2 py-0.5 align-middle"><span className="inline-block px-1.5 py-px rounded-full bg-blue-900/50 text-blue-300 border border-blue-700 font-medium">Prefer Serverless</span></td><td className="py-0.5">Serverless when possible, falls back to classic if paired with a Classic system.</td></tr>
          <tr><td className="pr-2 py-0.5 align-middle"><span className="inline-block px-1.5 py-px rounded-full bg-gray-800 text-gray-400 border border-gray-600 font-medium">Classic</span></td><td className="py-0.5">Dedicated cluster. Required for Direct JDBC connections.</td></tr>
        </tbody>
      </table>
    </>
  );
}
