import React from 'react';
import { ErrorBox } from '../components/ErrorBox';

export function SchedulesView({ 
  data, 
  loading, 
  error, 
  onEdit, 
  onDelete, 
  onClearError,
  renderCell 
}) {
  return (
    <>
      {error && error.action !== "setup_required" && <ErrorBox message={error.message} onClose={onClearError} />}
      <h2 className="text-2xl font-semibold text-rust-light mb-4">Schedules</h2>
      <button onClick={() => onEdit({})} className="mb-3 px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Add Schedule</button>
      {loading ? <p className="text-gray-400">Loading…</p> : (
        <table className="w-full border-collapse">
          <thead>
            <tr className="border-b-2 border-charcoal-200">
              <th className="text-left p-2 text-gray-400 font-medium">Name</th>
              <th className="text-left p-2 text-gray-400 font-medium">
                <a href="https://crontab.cronhub.io/" target="_blank" rel="noopener noreferrer" className="text-rust-light hover:text-rust underline">Cron</a>
              </th>
              <th className="text-left p-2 text-gray-400 font-medium">Timezone</th>
              <th className="text-left p-2 text-gray-400 font-medium">Enabled</th>
              <th className="text-left p-2 text-gray-400 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.map(row => (
              <tr key={row.id} className="border-b border-charcoal-200">
                <td className="p-2 text-gray-100">{row.name}</td>
                <td className="p-2 text-gray-200 font-mono text-sm">{renderCell('schedules', row, 'cron_expr')}</td>
                <td className="p-2 text-gray-200">{renderCell('schedules', row, 'timezone')}</td>
                <td className="p-2 text-gray-300">{row.enabled ? "✅" : "❌"}</td>
                <td className="p-2">
                  <button onClick={() => onEdit(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
                  <button onClick={() => onDelete('schedules', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500">Del</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
