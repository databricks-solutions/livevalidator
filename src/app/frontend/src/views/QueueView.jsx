import React from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { apiCall } from '../services/api';

export function QueueView({ 
  triggers, 
  queueStats, 
  onRefresh 
}) {
  const handleCancel = async (triggerId) => {
    if (confirm('Cancel this queued validation?')) {
      try {
        await apiCall('DELETE', `/api/triggers/${triggerId}`);
        onRefresh();
      } catch (e) {
        alert('Failed to cancel: ' + e.message);
      }
    }
  };

  return (
    <>
      {triggers.error && triggers.error.action !== "setup_required" && <ErrorBox message={triggers.error.message} onClose={triggers.clearError} />}
      <div className="mb-6">
        <h2 className="text-3xl font-bold text-rust-light mb-2">📋 Validation Queue</h2>
        <p className="text-gray-400">Active validation jobs in the queue</p>
      </div>

      {/* Queue Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <div className="bg-blue-900/20 border border-blue-700 rounded-lg p-4">
          <div className="text-blue-400 text-sm mb-1">⏳ Queued</div>
          <div className="text-3xl font-bold text-blue-300">
            {queueStats.data?.active?.queued || 0}
          </div>
        </div>
        <div className="bg-orange-900/20 border border-orange-700 rounded-lg p-4 animate-pulse">
          <div className="text-orange-400 text-sm mb-1">🔄 Running</div>
          <div className="text-3xl font-bold text-orange-300">
            {queueStats.data?.active?.running || 0}
          </div>
        </div>
        <div className="bg-green-900/20 border border-green-700 rounded-lg p-4">
          <div className="text-green-400 text-sm mb-1">✓ Completed (1h)</div>
          <div className="text-3xl font-bold text-green-300">
            {queueStats.data?.recent_1h?.succeeded || 0}
          </div>
        </div>
      </div>

      {triggers.loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="space-y-3">
          {triggers.data.length === 0 ? (
            <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-8 text-center text-gray-500">
              Queue is empty. No active validation jobs.
            </div>
          ) : (
            triggers.data.map((trigger) => (
              <div 
                key={trigger.id} 
                className={`bg-charcoal-500 border rounded-lg p-4 transition-all ${
                  trigger.status === 'running'
                    ? 'border-orange-600 shadow-lg shadow-orange-900/50 animate-pulse'
                    : 'border-blue-600 shadow-md shadow-blue-900/30'
                }`}
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <h3 className="text-lg font-semibold text-gray-100">
                        {trigger.entity_name || `${trigger.entity_type} #${trigger.entity_id}`}
                      </h3>
                      <span className={`px-3 py-1 text-xs font-medium rounded-full ${
                        trigger.status === 'running'
                          ? 'bg-orange-600 text-white'
                          : 'bg-blue-600 text-white'
                      }`}>
                        {trigger.status === 'running' ? '🔄 RUNNING' : '⏳ QUEUED'}
                      </span>
                      <span className={`px-2 py-1 text-xs rounded-full ${
                        trigger.entity_type === 'table'
                          ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                          : 'bg-purple-900/40 text-purple-300 border border-purple-700'
                      }`}>
                        {trigger.entity_type}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
                      <div>
                        <span className="text-gray-500">Requested by:</span>
                        <span className="text-gray-300 ml-2">{trigger.requested_by}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Priority:</span>
                        <span className="text-gray-300 ml-2">{trigger.priority}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Queued at:</span>
                        <span className="text-gray-300 ml-2">
                          {new Date(trigger.requested_at).toLocaleString()}
                        </span>
                      </div>
                      {trigger.started_at && (
                        <div>
                          <span className="text-gray-500">Started at:</span>
                          <span className="text-gray-300 ml-2">
                            {new Date(trigger.started_at).toLocaleString()}
                          </span>
                        </div>
                      )}
                      {trigger.worker_id && (
                        <div>
                          <span className="text-gray-500">Worker:</span>
                          <span className="text-gray-300 ml-2">{trigger.worker_id}</span>
                        </div>
                      )}
                      {trigger.attempts > 0 && (
                        <div>
                          <span className="text-gray-500">Attempts:</span>
                          <span className="text-gray-300 ml-2">{trigger.attempts}</span>
                        </div>
                      )}
                    </div>

                    {trigger.databricks_run_url && (
                      <div className="mt-3">
                        <a
                          href={trigger.databricks_run_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-2 px-3 py-1.5 bg-orange-600 hover:bg-orange-500 text-white text-sm rounded-md transition-colors"
                        >
                          <span>🔗 View Databricks Run</span>
                        </a>
                      </div>
                    )}
                  </div>

                  {trigger.status === 'queued' && (
                    <button
                      onClick={() => handleCancel(trigger.id)}
                      className="px-3 py-1.5 bg-red-600 hover:bg-red-500 text-white text-sm rounded-md transition-colors"
                    >
                      ✕ Cancel
                    </button>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      )}
    </>
  );
}
