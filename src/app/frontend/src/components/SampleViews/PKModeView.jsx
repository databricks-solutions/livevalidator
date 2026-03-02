import React from 'react';
import { DiffHighlight, ExpandableCell } from '../DiffComponents';

export function PKModeView({ samples, validation }) {
  if (!samples?.samples || samples.samples.length === 0) {
    return (
      <div className="p-3 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-sm">
          <strong>No column-level differences found.</strong> PK analysis completed but no differing columns were detected.
        </p>
        <p className="text-yellow-200 text-xs mt-2">
          This may indicate the differences are in excluded columns or due to data type coercion. 
          Check the Databricks notebook run for detailed investigation.
        </p>
        {validation?.databricks_run_url && (
          <a href={validation.databricks_run_url} target="_blank" rel="noopener noreferrer" className="inline-block mt-2 text-xs text-blue-400 hover:text-blue-300 underline">
            Open Notebook Run →
          </a>
        )}
      </div>
    );
  }
  
  const { pk_columns, samples: pkSamples } = samples;
  
  return (
    <div className="space-y-3">
      <div className="p-2 bg-purple-900/20 border border-purple-700 rounded">
        <p className="text-purple-300 text-xs">
          <strong>Showing {pkSamples.length} records</strong> where row values differ between source and target. 
          Primary keys: <span className="font-mono text-rust-light">{pk_columns.join(', ')}</span>
        </p>
      </div>
      
      {pkSamples.map((sample, idx) => (
        <div key={idx} className="border border-charcoal-300 rounded-lg overflow-hidden">
          <div className="bg-charcoal-400 px-3 py-2 border-b border-charcoal-300">
            <span className="text-gray-300 font-semibold text-xs">Record #{idx + 1} — </span>
            {Object.entries(sample.pk).map(([key, value], pkIdx) => (
              <span key={key}>
                <span className="text-gray-400 text-xs">{key}:</span>
                <span className="text-rust-light font-mono text-xs ml-1 mr-3">{value !== null ? String(value) : 'null'}</span>
                {pkIdx < Object.keys(sample.pk).length - 1 && <span className="text-gray-600">•</span>}
              </span>
            ))}
          </div>
          
          <table className="w-full">
            <thead className="bg-charcoal-400/50">
              <tr>
                <th className="px-3 py-1.5 text-left text-xs text-gray-300 w-1/4">Column</th>
                <th className="px-3 py-1.5 text-left text-xs text-blue-300 w-3/8">
                  <span className="flex items-center gap-1.5">
                    <span className="inline-block w-2 h-2 bg-blue-500 rounded"></span>
                    Source Value
                  </span>
                </th>
                <th className="px-3 py-1.5 text-left text-xs text-purple-300 w-3/8">
                  <span className="flex items-center gap-1.5">
                    <span className="inline-block w-2 h-2 bg-purple-500 rounded"></span>
                    Target Value
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {sample.differences.map((diff, diffIdx) => (
                <tr key={diffIdx} className="border-t border-charcoal-300">
                  <td className="px-3 py-1.5 text-xs font-mono text-gray-300 align-top">{diff.column}</td>
                  <td className="px-3 py-1.5 text-xs text-blue-200 bg-blue-900/10 align-top">
                    <div className="font-mono"><ExpandableCell value={diff.source_value} /></div>
                  </td>
                  <td className="px-3 py-1.5 text-xs text-purple-200 bg-purple-900/10 align-top">
                    <div className="font-mono"><DiffHighlight source={diff.source_value} target={diff.target_value} /></div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}
