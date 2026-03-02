import React from 'react';
import { useCurrentUser } from '../context/UserContext';

export function Sidebar({ view, setView, setupRequired }) {
  const currentUser = useCurrentUser();
  
  const viewLabels = {
    'results': 'Results',
    'dashboard': 'Dashboard',
    'analysis': 'Analysis',
    'tables': 'Tables',
    'queries': 'Queries',
    'queue': 'Queue',
    'configuration': 'Configs',
    'type-mappings': 'Type Mappings',
    'schedules': 'Schedules',
    'systems': 'Systems',
    'admin': 'Admin',
    'setup': 'Setup'
  };

  const mainViews = ['results','dashboard','analysis','tables','queries','queue','configuration','type-mappings','schedules','systems'];
  // Show Admin/Setup to CAN_MANAGE users, OR if DB setup is required
  const bottomViews = (currentUser?.role === 'CAN_MANAGE' || setupRequired) ? ['admin', 'setup'] : [];

  return (
    <div className="w-48 border-r border-charcoal-200 py-5 fixed top-0 left-0 bottom-0 flex flex-col bg-charcoal-600">
      <div className="px-4 mb-6">
        <h2 className="text-lg font-bold text-rust inline-block">LiveValidator</h2>
        <span className="ml-2 inline-block px-2 py-0.5 text-[10px] font-bold tracking-wider bg-gradient-to-r from-purple-500 to-purple-700 text-white rounded-full border border-purple-400 shadow-lg shadow-purple-500/50 transform -rotate-3 relative top-[-2px]">
          BETA
        </span>
      </div>
      <div className="flex-1 overflow-y-auto">
        {mainViews.map(v => (
          <div
            key={v}
            onClick={() => setView(v)}
            className={`px-4 py-3 cursor-pointer text-gray-200 border-l-4 transition-all ${
              view === v 
                ? 'border-rust bg-charcoal-500 font-semibold' 
                : 'border-transparent hover:bg-charcoal-500/50 hover:border-charcoal-300'
            }`}
          >
            {viewLabels[v]}
          </div>
        ))}
      </div>
      {bottomViews.length > 0 && (
        <div className="border-t border-charcoal-200 mt-auto">
          {bottomViews.map(v => (
            <div
              key={v}
              onClick={() => setView(v)}
              className={`px-4 py-3 cursor-pointer text-gray-200 border-l-4 transition-all ${
                view === v 
                  ? 'border-rust bg-charcoal-500 font-semibold' 
                  : 'border-transparent hover:bg-charcoal-500/50 hover:border-charcoal-300'
              }`}
            >
              {viewLabels[v]}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

