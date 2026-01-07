import React, { useState, useRef } from 'react';
import { parseCSV } from '../../utils/csvParser';
import { tableService, queryService } from '../../services/api';

export function UploadCSVModal({ type, systems, schedules, onClose, onUpload }) {
  const [srcSystemId, setSrcSystemId] = useState(systems[0]?.id || 1);
  const [tgtSystemId, setTgtSystemId] = useState(systems[1]?.id || systems[0]?.id || 2);
  const [file, setFile] = useState(null);
  const [parsed, setParsed] = useState(null);
  const [errors, setErrors] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  
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

  const handleFileSelect = (selectedFile) => {
    if (!selectedFile) return;
    setFile(selectedFile);
    setErrors([]);
    setParsed(null);
    
    parseCSV(selectedFile, type, schedules, (validRows, validationErrors) => {
      setErrors(validationErrors);
      setParsed(validRows);
    });
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFileSelect(e.dataTransfer.files[0]);
    }
  };

  const handleUpload = async () => {
    if (!parsed || errors.length > 0) return;
    
    setUploading(true);
    try {
      const uploadFn = type === 'tables' ? tableService.bulkUpload : queryService.bulkUpload;
      const result = await uploadFn(srcSystemId, tgtSystemId, parsed);
      
      if (result.errors && result.errors.length > 0) {
        alert(`Upload completed with errors:\n${result.errors.map(e => `Row ${e.row}: ${e.error}`).join('\n')}`);
      }
      
      if (result.updated && result.updated.length > 0) {
        const updatedNames = result.updated.map(u => u.name).join(', ');
        if (!confirm(`${result.updated.length} existing records will be updated: ${updatedNames}. Continue?`)) {
          setUploading(false);
          return;
        }
      }
      
      onUpload();
      onClose();
    } catch (err) {
      alert(`Upload failed: ${err.message}`);
    } finally {
      setUploading(false);
    }
  };

  return (
    <div ref={backdropRef} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} className="fixed inset-0 bg-black/75 flex items-center justify-center z-50">
      <div className={`bg-charcoal-500 rounded-xl w-full ${parsed ? 'max-w-[90vw]' : 'max-w-4xl'} shadow-2xl max-h-[90vh] overflow-y-auto border border-charcoal-200`}>
        <div className="sticky top-0 bg-charcoal-500 px-6 py-4 border-b border-charcoal-200 flex justify-between items-center">
          <h3 className="m-0 text-rust text-lg font-semibold">Upload CSV - {type === 'tables' ? 'Tables' : 'Queries'}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200 text-2xl leading-none border-0 bg-transparent cursor-pointer">&times;</button>
        </div>

        <div className="px-6 py-4">
          {/* System Selection */}
          <div className="mb-6 pb-6 border-b border-charcoal-200">
            <h4 className="text-rust-light font-semibold mb-3">System Configuration</h4>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-gray-400 text-sm mb-1">Source System</label>
                <select value={srcSystemId} onChange={e => setSrcSystemId(parseInt(e.target.value))} className="w-full px-3 py-2 bg-charcoal-600 text-gray-200 border border-charcoal-200 rounded-md">
                  {systems.map(s => <option key={s.id} value={s.id}>{s.name} ({s.kind})</option>)}
                </select>
              </div>
              <div>
                <label className="block text-gray-400 text-sm mb-1">Target System</label>
                <select value={tgtSystemId} onChange={e => setTgtSystemId(parseInt(e.target.value))} className="w-full px-3 py-2 bg-charcoal-600 text-gray-200 border border-charcoal-200 rounded-md">
                  {systems.map(s => <option key={s.id} value={s.id}>{s.name} ({s.kind})</option>)}
                </select>
              </div>
            </div>
          </div>

          {/* CSV Format Instructions */}
          <div className="mb-6 pb-6 border-b border-charcoal-200">
            <h4 className="text-rust-light font-semibold mb-3">📋 CSV Format Requirements</h4>
            <div className="bg-charcoal-600 rounded-lg p-4 text-sm">
              <p className="text-gray-300 mb-3 font-medium">Your CSV file must include a header row with the following columns:</p>
              
              {type === 'tables' ? (
                <>
                  <div className="mb-3">
                    <p className="text-rust-light font-semibold mb-1">✅ Required Headers:</p>
                    <ul className="text-gray-300 ml-4 space-y-1">
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded text-purple-400">src_schema</code> - Source schema name</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded text-purple-400">src_table</code> - Source table name</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded text-purple-400">schedule_name</code> - Schedule to bind to (must exist)</li>
                    </ul>
                  </div>
                  <div>
                    <p className="text-rust-light font-semibold mb-1">🔧 Optional Headers:</p>
                    <ul className="text-gray-400 ml-4 space-y-1 text-xs">
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">name</code> - Display name (defaults to src_schema.src_table)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">tgt_schema</code> - Target schema (defaults to src_schema)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">tgt_table</code> - Target table (defaults to src_table)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">is_active</code> - true/false (defaults to true)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">compare_mode</code> - except_all, union, intersect (defaults to except_all)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">pk_columns</code> - Comma-separated primary key columns</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">watermark_column</code> - Watermark column name</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">include_columns</code> - Comma-separated columns to include</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">exclude_columns</code> - Comma-separated columns to exclude</li>
                    </ul>
                  </div>
                </>
              ) : (
                <>
                  <div className="mb-3">
                    <p className="text-rust-light font-semibold mb-1">✅ Required Headers:</p>
                    <ul className="text-gray-300 ml-4 space-y-1">
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded text-purple-400">sql</code> - SQL query to execute</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded text-purple-400">schedule_name</code> - Schedule to bind to (must exist)</li>
                    </ul>
                  </div>
                  <div>
                    <p className="text-rust-light font-semibold mb-1">🔧 Optional Headers:</p>
                    <ul className="text-gray-400 ml-4 space-y-1 text-xs">
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">name</code> - Display name (defaults to "Query [row#]")</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">is_active</code> - true/false (defaults to true)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">compare_mode</code> - except_all, primary_key (defaults to except_all)</li>
                      <li><code className="bg-charcoal-700 px-2 py-0.5 rounded">pk_columns</code> - Comma-separated primary key columns (used if compare_mode is 'primary_key')</li>
                    </ul>
                  </div>
                </>
              )}
              
              <div className="mt-3 pt-3 border-t border-charcoal-400">
                <p className="text-gray-500 text-xs">
                  💡 <strong>Tips:</strong> Array fields (pk_columns, include_columns, exclude_columns) should be comma-separated within the CSV cell. 
                  Existing records with matching names will be updated.
                </p>
              </div>
            </div>
          </div>

          {/* File Upload */}
          <div className="mb-6">
            <h4 className="text-rust-light font-semibold mb-3">📁 Upload File</h4>
            <div
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
              className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
                dragActive ? 'border-rust bg-charcoal-600' : 'border-charcoal-200 bg-charcoal-600'
              }`}
            >
              <input
                type="file"
                accept=".csv"
                onChange={(e) => handleFileSelect(e.target.files[0])}
                className="hidden"
                id="csv-upload"
              />
              <label htmlFor="csv-upload" className="cursor-pointer">
                <div className="text-gray-300 mb-2 text-lg">
                  {file ? `📄 ${file.name}` : '📁 Drag & drop CSV file here or click to browse'}
                </div>
                <div className="text-gray-500 text-sm">
                  CSV files must include headers • {parsed ? `${parsed.length} rows parsed` : 'No file selected'}
                </div>
              </label>
            </div>
          </div>

          {/* Errors */}
          {errors.length > 0 && (
            <div className="mb-6 p-4 bg-red-900/30 border border-red-700 rounded-md">
              <h4 className="text-red-400 font-semibold mb-2">❌ Validation Errors</h4>
              <ul className="text-red-300 text-sm list-disc list-inside">
                {errors.map((err, i) => <li key={i}>{err}</li>)}
              </ul>
            </div>
          )}

          {/* Preview */}
          {parsed && parsed.length > 0 && errors.length === 0 && (
            <div className="mb-6">
              <h4 className="text-rust-light font-semibold mb-3">✅ Preview ({parsed.length} rows)</h4>
              <div className="bg-charcoal-600 rounded-md p-4 max-h-96 overflow-auto">
                <table className="w-full text-sm text-gray-200">
                  <thead className="text-gray-400 border-b border-charcoal-200">
                    <tr>
                      <th className="text-left p-2">Row</th>
                      {Object.keys(parsed[0] || {}).map(key => (
                        <th key={key} className="text-left p-2">{key}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {parsed.map((row, i) => (
                      <tr key={i} className="border-b border-charcoal-300/30">
                        <td className="p-2">{i + 1}</td>
                        {Object.keys(parsed[0] || {}).map(key => (
                          <td key={key} className="p-2 max-w-xs truncate" title={row[key]}>
                            {Array.isArray(row[key]) ? row[key].join(', ') : row[key]}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        <div className="sticky bottom-0 bg-charcoal-500 px-6 py-4 border-t border-charcoal-200 flex justify-end gap-3">
          <button onClick={onClose} className="px-4 py-2 bg-charcoal-400 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-300">Cancel</button>
          <button 
            onClick={handleUpload} 
            disabled={!parsed || errors.length > 0 || uploading}
            className="px-4 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {uploading ? 'Uploading...' : `💾 Upload ${parsed?.length || 0} Rows`}
          </button>
        </div>
      </div>
    </div>
  );
}

