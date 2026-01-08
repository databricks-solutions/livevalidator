import React, { useState, useRef } from 'react';

export function BindScheduleModal({ entityType, entityId, schedules, onSave, onClose }) {
  const [scheduleId, setScheduleId] = useState(schedules[0]?.id || '');
  
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
  
  const handleSave = () => {
    if (scheduleId) {
      onSave(scheduleId);
    }
  };
  
  return (
    <div ref={backdropRef} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} className="fixed inset-0 bg-black/75 flex items-center justify-center z-50">
      <div onClick={(e)=>e.stopPropagation()} className="bg-charcoal-500 rounded-xl w-full max-w-md shadow-2xl border border-charcoal-200">
        <div className="border-b border-charcoal-200 px-4 py-3 bg-charcoal-400">
          <h3 className="m-0 text-rust text-lg font-semibold">Bind Schedule</h3>
        </div>
        <div className="p-4">
          <div className="mb-4">
            <label className="block mb-1.5 font-medium text-gray-300 text-sm">Select Schedule *</label>
            <select 
              value={scheduleId} 
              onChange={e => setScheduleId(+e.target.value)} 
              className="w-full px-3 py-2.5 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500"
            >
              {schedules.map(s => (
                <option key={s.id} value={s.id}>
                  {s.name} - {s.cron_expr}
                </option>
              ))}
            </select>
            <p className="text-gray-500 text-xs mt-1">This {entityType} will run on this schedule</p>
          </div>
        </div>
        <div className="border-t border-charcoal-200 px-4 py-3 flex gap-2 justify-end bg-charcoal-400">
          <button onClick={onClose} className="px-3 py-2 bg-charcoal-700 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-600">Cancel</button>
          <button onClick={handleSave} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Bind</button>
        </div>
      </div>
    </div>
  );
}

