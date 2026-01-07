import React, { useState, useRef, useEffect } from 'react';
import { API } from '../../services/api';

/**
 * Validates a cron expression is standard 5-field Unix format.
 * Returns null if valid, or an error message string if invalid.
 */
function validateCronExpression(cronExpr) {
  if (!cronExpr || !cronExpr.trim()) {
    return 'Cron expression is required';
  }
  
  const trimmed = cronExpr.trim();
  const fields = trimmed.split(/\s+/);
  
  // Check for Quartz-specific ? character
  if (trimmed.includes('?')) {
    return 'Invalid cron format: "?" is not supported. Please use standard Unix cron syntax (use * instead of ?)';
  }
  
  // Must be exactly 5 fields for standard Unix cron
  if (fields.length === 6) {
    return 'Invalid cron format: 6-field expressions (with seconds) are not supported. Please use standard 5-field Unix cron format: minute hour day-of-month month day-of-week';
  }
  
  if (fields.length === 7) {
    return 'Invalid cron format: 7-field expressions (Quartz format with seconds and year) are not supported. Please use standard 5-field Unix cron format: minute hour day-of-month month day-of-week';
  }
  
  if (fields.length !== 5) {
    return `Invalid cron format: expected 5 fields (minute hour day-of-month month day-of-week), but got ${fields.length}`;
  }
  
  return null;
}

export function ScheduleModal({ schedule, onSave, onClose }) {
  const [form, setForm] = useState(() => ({
    name: schedule?.name || "New Schedule",
    cron_expr: schedule?.cron_expr || "0 0 * * *",
    timezone: schedule?.timezone || "UTC",
    enabled: schedule?.enabled ?? true,
    version: schedule?.version || 0
  }));
  
  const [timezones, setTimezones] = useState(['UTC']);
  const [cronError, setCronError] = useState(null);
  
  useEffect(() => {
    API.getTimezones().then(setTimezones).catch(err => {
      console.error('Failed to load timezones:', err);
    });
  }, []);
  
  const handleSave = async () => {
    const error = validateCronExpression(form.cron_expr);
    if (error) {
      setCronError(error);
      return;
    }
    setCronError(null);
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
  
  return (
    <div ref={backdropRef} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} className="fixed inset-0 bg-black/75 flex items-center justify-center z-50">
      <div onClick={(e)=>e.stopPropagation()} className="bg-charcoal-500 rounded-xl w-full max-w-lg shadow-2xl border border-charcoal-200">
        <div className="border-b border-charcoal-200 px-4 py-3 bg-charcoal-400">
          <h3 className="m-0 text-rust text-lg font-semibold">{schedule ? "Edit Schedule" : "New Schedule"}</h3>
        </div>
        <div className="p-4 max-h-[70vh] overflow-y-auto">
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Name</label>
            <input value={form.name} onChange={e=>setForm({...form, name:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" />
          </div>
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Cron Expression</label>
            <input 
              value={form.cron_expr} 
              onChange={e => {
                setForm({...form, cron_expr: e.target.value});
                if (cronError) setCronError(null);
              }} 
              className={`w-full px-2 py-2 rounded-md border bg-charcoal-400 text-gray-100 font-mono focus:outline-none focus:ring-2 ${cronError ? 'border-red-500 focus:ring-red-500' : 'border-charcoal-200 focus:ring-purple-500'}`} 
              placeholder="0 0 * * *" 
            />
            <p className="mt-1 text-xs text-gray-500">Standard 5-field Unix format: minute hour day-of-month month day-of-week</p>
            {cronError && (
              <div className="mt-2 p-2 bg-red-900/30 border border-red-500/50 rounded-md">
                <p className="text-red-400 text-sm flex items-start gap-2">
                  <span className="text-red-500 mt-0.5">⚠</span>
                  <span>{cronError}</span>
                </p>
              </div>
            )}
          </div>
          <div className="grid grid-cols-2 gap-3 mb-3">
            <div>
              <label className="block mb-1 font-medium text-gray-400 text-sm">
                Timezone <span className="text-gray-500 text-xs">(IANA format)</span>
              </label>
              <input 
                list="timezones-list" 
                value={form.timezone} 
                onChange={e=>setForm({...form, timezone:e.target.value})} 
                className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" 
                placeholder="e.g., America/New_York, America/Phoenix" 
              />
              <datalist id="timezones-list">
                {timezones.map(tz => <option key={tz} value={tz} />)}
              </datalist>
            </div>
            <div>
              <label className="block mb-1 font-medium text-gray-400 text-sm">Enabled</label>
              <label className="flex items-center gap-2 px-2 py-2">
                <input type="checkbox" checked={form.enabled} onChange={e=>setForm({...form, enabled:e.target.checked})} className="w-4 h-4" />
                <span className="text-gray-300">{form.enabled ? "Yes" : "No"}</span>
              </label>
            </div>
          </div>
        </div>
        <div className="border-t border-charcoal-200 px-4 py-3 flex gap-2 justify-end bg-charcoal-400">
          <button onClick={onClose} className="px-3 py-2 bg-charcoal-700 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-600">Cancel</button>
          <button onClick={handleSave} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Save</button>
        </div>
      </div>
    </div>
  );
}

