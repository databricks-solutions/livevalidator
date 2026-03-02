import React, { useState } from 'react';
import {
  PKModeView,
  PKRowCountMismatchView,
  ExceptAllCountMismatchView,
  PKPendingView,
  ExceptAllModeView,
} from '../SampleViews';

export function SampleDifferencesContent({ validation }) {
  if (!validation) return null;

  let samples = validation.sample_differences;
  if (typeof samples === 'string') {
    try { samples = JSON.parse(samples); } catch { samples = null; }
  }
  
  const isPKMode = samples?.mode === 'primary_key';
  const isRowCountMismatch = samples?.mode === 'row_count_mismatch';
  const isExceptAllCountMismatch = samples?.mode === 'row_count_mismatch_except_all';
  const isExceptAllMode = Array.isArray(samples) && samples.length > 0;
  const isPKPending = validation.compare_mode === 'primary_key' && isExceptAllMode;
  const isExceptAllCountPending = validation.compare_mode === 'except_all' && !validation.row_count_match && !isExceptAllMode && !isExceptAllCountMismatch;
  const isPKCountPending = validation.compare_mode === 'primary_key' && !validation.row_count_match && !isRowCountMismatch;

  if (isPKMode) return <PKModeView samples={samples} validation={validation} />;
  if (isRowCountMismatch) return <PKRowCountMismatchView samples={samples} validation={validation} />;
  if (isExceptAllCountMismatch) return <ExceptAllCountMismatchView samples={samples} validation={validation} />;
  if (isPKPending) return <PKPendingView samples={samples} validation={validation} />;
  if (isExceptAllMode) return <ExceptAllModeView samples={samples} validation={validation} />;
  
  if (isExceptAllCountPending || isPKCountPending) {
    return (
      <div className="p-4 bg-charcoal-400 border border-charcoal-300 rounded-lg">
        <p className="text-gray-300 mb-2"><span className="font-semibold">Row count mismatch detected</span></p>
        <p className="text-gray-400 text-sm">
          Source: {validation.row_count_source?.toLocaleString()} rows • Target: {validation.row_count_target?.toLocaleString()} rows
        </p>
        <p className="text-gray-500 text-sm mt-3">Analysis data may still be processing. Check the validation notebook for more details.</p>
      </div>
    );
  }
  
  return <p className="text-gray-400">No sample data available</p>;
}

export function SampleDifferencesModal({ validation, onClose }) {
  const [maximized, setMaximized] = useState(false);
  if (!validation) return null;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className={`bg-charcoal-500 border border-charcoal-200 rounded-lg p-6 overflow-hidden flex flex-col transition-all ${
          maximized ? 'w-full h-full max-w-none max-h-none m-0 rounded-none' : 'max-w-6xl w-full mx-4 max-h-[90vh]'
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-between items-start mb-3">
          <div>
            <h3 className="text-lg font-bold text-rust-light">Sample Differences</h3>
            <p className="text-gray-400 text-xs mt-1">
              {validation.entity_name} • {validation.compare_mode} mode
              {validation.rows_different != null && ` • ${validation.rows_different.toLocaleString()} total differences`}
              {!validation.row_count_match && ' • Row count mismatch'}
            </p>
          </div>
          <div className="flex items-center gap-1">
            <button onClick={(e) => { e.stopPropagation(); setMaximized(!maximized); }} className="text-gray-400 hover:text-gray-200 leading-none px-2 py-1" title={maximized ? 'Restore' : 'Maximize'}>
              {maximized ? (
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><rect x="8" y="8" width="12" height="12" strokeWidth="2" rx="1" /><path d="M4 16V5a1 1 0 011-1h11" strokeWidth="2" /></svg>
              ) : (
                <svg className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24"><path d="M4 14v6h6M20 10V4h-6M4 20L10 14M20 4L14 10" /></svg>
              )}
            </button>
            <button onClick={onClose} className="text-gray-400 hover:text-gray-200 text-2xl leading-none px-2">×</button>
          </div>
        </div>
        <div className="flex-1 overflow-auto">
          <SampleDifferencesContent validation={validation} />
        </div>
        <div className="mt-4 pt-4 border-t border-charcoal-300 flex justify-end">
          <button onClick={onClose} className="px-4 py-2 bg-charcoal-600 text-gray-300 border border-charcoal-300 rounded hover:bg-charcoal-500 transition-all">Close</button>
        </div>
      </div>
    </div>
  );
}
