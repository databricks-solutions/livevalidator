import React from 'react';

export function Sidebar({ view, setView }) {
  return (
    <div className="w-48 border-r border-charcoal-200 py-5 fixed top-0 left-0 bottom-0 overflow-y-auto bg-charcoal-600">
      <h2 className="px-4 text-lg font-bold text-rust mb-6">LiveValidator</h2>
      {['tables','queries','schedules','systems','setup'].map(v => (
        <div
          key={v}
          onClick={() => setView(v)}
          className={`px-4 py-3 cursor-pointer text-gray-200 border-l-4 transition-all ${
            view === v 
              ? 'border-rust bg-charcoal-500 font-semibold' 
              : 'border-transparent hover:bg-charcoal-500/50 hover:border-charcoal-300'
          }`}
        >
          {v.charAt(0).toUpperCase() + v.slice(1)}
        </div>
      ))}
    </div>
  );
}

