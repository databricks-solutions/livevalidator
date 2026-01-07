import React from 'react';

export const Checkbox = ({ checked, onChange, className = '' }) => {
  return (
    <input
      type="checkbox"
      checked={checked}
      onChange={onChange}
      className={`appearance-none cursor-pointer w-5 h-5 rounded border-2 border-gray-400 bg-transparent checked:bg-rust-light checked:border-rust-light focus:ring-2 focus:ring-rust-light focus:ring-offset-0 transition-colors relative checked:after:content-['✓'] checked:after:absolute checked:after:inset-0 checked:after:text-white checked:after:text-sm checked:after:flex checked:after:items-center checked:after:justify-center ${className}`}
    />
  );
};

