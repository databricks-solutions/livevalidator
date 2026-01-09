import React, { useRef } from 'react';

export function VersionConflictDialog({ current, onRefresh, onCancel }) {
  const backdropRef = useRef(null);
  const mouseDownTarget = useRef(null);
  
  const handleMouseDown = (e) => {
    mouseDownTarget.current = e.target;
  };
  
  const handleMouseUp = (e) => {
    if (mouseDownTarget.current === backdropRef.current && e.target === backdropRef.current) {
      onCancel();
    }
    mouseDownTarget.current = null;
  };
  
  return (
    <div ref={backdropRef} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} className="fixed inset-0 bg-black/75 flex items-center justify-center z-50">
      <div className="bg-charcoal-500 rounded-xl w-full max-w-lg shadow-2xl p-5 border border-charcoal-200">
        <h3 className="mt-0 text-rust text-xl font-semibold">Data Changed</h3>
        <p className="text-gray-200">Another user modified this record. Current version: {current?.version}</p>
        <div className="flex gap-2 justify-end mt-4">
          <button onClick={onCancel} className="px-3 py-2 bg-charcoal-400 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-300">Cancel</button>
          <button onClick={onRefresh} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Refresh & Retry</button>
        </div>
      </div>
    </div>
  );
}

