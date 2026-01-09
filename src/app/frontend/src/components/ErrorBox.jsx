import React from 'react';

export function ErrorBox({ message, onClose }) {
  return (
    <div className="bg-red-900/30 border border-red-700 text-red-200 px-4 py-3 rounded-md mb-4 flex justify-between items-start">
      <div className="flex-1">
        <strong className="font-bold">Error: </strong>
        <span>{message}</span>
      </div>
      <button 
        onClick={onClose} 
        className="text-red-300 hover:text-red-100 font-bold text-lg leading-none ml-4"
      >
        &times;
      </button>
    </div>
  );
}

