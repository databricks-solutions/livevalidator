import React from 'react';

function parseSchemaDetails(details) {
  if (!details) return null;
  if (typeof details === 'string') {
    try { return JSON.parse(details); } catch { return null; }
  }
  return details;
}

export function SchemaMismatchAlert({ validation }) {
  if (!validation || validation.schema_match !== false) return null;

  const details = parseSchemaDetails(validation.schema_details);
  const missing = details?.columns_missing ?? [];
  const extra = details?.columns_extra ?? [];
  if (missing.length === 0 && extra.length === 0) return null;

  return (
    <div className="p-3 bg-red-900/20 border border-red-700 rounded-lg">
      <p className="text-red-300 text-xs font-semibold mb-2">
        Schema mismatch — these columns were excluded from the row comparison
      </p>
      <div className="space-y-1.5 text-xs">
        {missing.length > 0 && (
          <div>
            <span className="text-red-200 font-medium">Missing in target ({missing.length}):</span>{' '}
            {missing.map((col) => (
              <span key={col} className="inline-block font-mono text-red-100 bg-red-900/40 rounded px-1.5 py-0.5 mr-1 mb-1">{col}</span>
            ))}
          </div>
        )}
        {extra.length > 0 && (
          <div>
            <span className="text-amber-200 font-medium">Extra in target ({extra.length}):</span>{' '}
            {extra.map((col) => (
              <span key={col} className="inline-block font-mono text-amber-100 bg-amber-900/40 rounded px-1.5 py-0.5 mr-1 mb-1">{col}</span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
