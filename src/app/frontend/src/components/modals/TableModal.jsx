import React, { useState, useRef, useEffect } from 'react';
import { TagInput } from '../TagInput';

export function TableModal({ table, systems, schedules, onSave, onClose }) {
  // Combine schema.table for display
  const getSrcTableFull = () => {
    if (!table) return "";
    const schema = table.src_schema || "";
    const tbl = table.src_table || "";
    return schema && tbl ? `${schema}.${tbl}` : tbl;
  };
  
  const getTgtTableFull = () => {
    if (!table) return "";
    const schema = table.tgt_schema || "";
    const tbl = table.tgt_table || "";
    return schema && tbl ? `${schema}.${tbl}` : tbl;
  };
  
  const [srcTableFull, setSrcTableFull] = useState(getSrcTableFull());
  const [tgtTableFull, setTgtTableFull] = useState(getTgtTableFull());
  const [name, setName] = useState(table?.name || "");
  
  const [form, setForm] = useState(() => ({
    src_system_id: table?.src_system_id || (systems[0]?.id || 1),
    tgt_system_id: table?.tgt_system_id || (systems[1]?.id || 2),
    compare_mode: table?.compare_mode || "except_all",
    pk_columns: table?.pk_columns || [],
    watermark_filter: table?.watermark_filter || "",
    exclude_columns: table?.exclude_columns || [],
    version: table?.version || 0
  }));
  
  const [selectedSchedules, setSelectedSchedules] = useState([]);
  const [tags, setTags] = useState([]);
  const [allTags, setAllTags] = useState([]);
  
  // Fetch existing bindings and tags for this table
  useEffect(() => {
    if (table?.id) {
      fetch(`/api/bindings/table/${table.id}`)
        .then(r => r.json())
        .then(bindings => {
          const scheduleIds = bindings.map(b => b.schedule_id);
          setSelectedSchedules(scheduleIds);
        })
        .catch(() => setSelectedSchedules([]));
      
      fetch(`/api/tags/entity/table/${table.id}`)
        .then(r => r.json())
        .then(data => setTags(data.map(t => t.name)))
        .catch(() => setTags([]));
    } else {
      setTags(table?.tags || []);
    }
    
    // Fetch all existing tags for autocomplete
    fetch('/api/tags')
      .then(r => r.json())
      .then(data => setAllTags(data.map(t => t.name)))
      .catch(() => setAllTags([]));
  }, [table?.id]);
  
  // Auto-populate target and name when source changes
  const handleSrcChange = (value) => {
    setSrcTableFull(value);
    // Auto-populate target if it's empty or was auto-filled
    if (!tgtTableFull || tgtTableFull === srcTableFull) {
      setTgtTableFull(value);
    }
    // Auto-populate name if it's empty or was auto-filled
    if (!name || name === srcTableFull || name === "New Table") {
      setName(value);
    }
  };
  
  const handleSave = async () => {
    // Parse schema.table format
    const parseSchematable = (full) => {
      const parts = full.split('.');
      if (parts.length >= 2) {
        return { schema: parts.slice(0, -1).join('.'), table: parts[parts.length - 1] };
      }
      return { schema: '', table: full };
    };
    
    const src = parseSchematable(srcTableFull);
    const tgt = parseSchematable(tgtTableFull);
    
    const payload = {
      ...form,
      name,
      entity_type: "table",
      src_schema: src.schema,
      src_table: src.table,
      tgt_schema: tgt.schema,
      tgt_table: tgt.table
    };
    
    await onSave(payload, selectedSchedules, tags);
  };
  
  const toggleSchedule = (scheduleId) => {
    setSelectedSchedules(prev => 
      prev.includes(scheduleId) 
        ? prev.filter(id => id !== scheduleId)
        : [...prev, scheduleId]
    );
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
      <div onClick={(e)=>e.stopPropagation()} className="bg-charcoal-500 rounded-xl w-full max-w-3xl shadow-2xl border border-charcoal-200">
        <div className="border-b border-charcoal-200 px-4 py-3 bg-charcoal-400">
          <h3 className="m-0 text-rust text-lg font-semibold">{table ? "Edit Table" : "New Table"}</h3>
        </div>
        <div className="p-4 max-h-[70vh] overflow-y-auto">
          {/* Source System & Table */}
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Source Table <span className="text-gray-500 text-xs">(include schema: schema.table)</span></label>
            <input 
              value={srcTableFull} 
              onChange={e=>handleSrcChange(e.target.value)} 
              className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" 
              placeholder="e.g., my_schema.my_table"
            />
          </div>
          {/* Source & Target Systems side-by-side */}
          <div className="grid grid-cols-2 gap-3 mb-3">
            <div>
              <label className="block mb-1 font-medium text-gray-400 text-sm">Source System</label>
              <select value={form.src_system_id} onChange={e=>setForm({...form, src_system_id:+e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500">
                {systems.map(s=><option key={s.id} value={s.id}>{s.name}</option>)}
              </select>
            </div>
            <div>
              <label className="block mb-1 font-medium text-gray-400 text-sm">Target System</label>
              <select value={form.tgt_system_id} onChange={e=>setForm({...form, tgt_system_id:+e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500">
                {systems.map(s=><option key={s.id} value={s.id}>{s.name}</option>)}
              </select>
            </div>
          </div>
          
          {/* Target Table */}
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Target Table <span className="text-gray-500 text-xs">(include schema: schema.table)</span></label>
            <input 
              value={tgtTableFull} 
              onChange={e=>setTgtTableFull(e.target.value)} 
              className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" 
              placeholder="e.g., my_schema.my_table"
            />
          </div>
          
          {/* Name - moved lower */}
          <div className="mb-3 pb-3 border-t border-charcoal-200 pt-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Name <span className="text-gray-500 text-xs">(defaults to source table)</span></label>
            <input 
              value={name} 
              onChange={e=>setName(e.target.value)} 
              className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" 
            />
          </div>
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Compare Mode</label>
            <select value={form.compare_mode} onChange={e=>setForm({...form, compare_mode:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500">
              <option value="except_all">Except All</option>
              <option value="primary_key">Primary Key</option>
            </select>
          </div>
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Watermark Filter</label>
            <input value={form.watermark_filter} onChange={e=>setForm({...form, watermark_filter:e.target.value})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="e.g., created_at > '2024-01-01' OR status = 'active'" />
            <p className="text-gray-500 text-xs mt-1">Optional WHERE clause to filter rows before comparison (applied to both source and target)</p>
          </div>
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Primary Key Columns (comma-separated)</label>
            <input value={Array.isArray(form.pk_columns)?form.pk_columns.join(','):''} onChange={e=>setForm({...form, pk_columns:e.target.value.split(',').map(s=>s.trim()).filter(Boolean)})} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="id, user_id" />
          </div>
          <div className="mb-3">
            <label className="block mb-1 font-medium text-gray-400 text-sm">Exclude Columns (comma-separated)</label>
            <textarea value={Array.isArray(form.exclude_columns)?form.exclude_columns.join(', '):''} onChange={e=>setForm({...form, exclude_columns:e.target.value.split(',').map(s=>s.trim()).filter(Boolean)})} rows={3} className="w-full px-2 py-2 rounded-md border border-charcoal-200 bg-charcoal-400 text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500" placeholder="column1, column2, column3" />
          </div>
          
          {/* Tags */}
          <div className="mb-3 pb-3 border-t border-charcoal-200 pt-3">
            <label className="block mb-2 font-medium text-gray-400 text-sm">Tags</label>
            <TagInput 
              tags={tags}
              allTags={allTags}
              onChange={setTags}
              placeholder="Add tags (press Enter)..."
            />
          </div>
          
          {/* Schedule Bindings */}
          <div className="mb-3 pb-3 border-t border-charcoal-200 pt-3">
            <label className="block mb-2 font-medium text-gray-400 text-sm">Schedules</label>
            <div className="max-h-32 overflow-y-auto bg-charcoal-600 rounded-md p-2">
              {schedules.length === 0 ? (
                <p className="text-gray-500 text-xs">No schedules available</p>
              ) : (
                schedules.map(schedule => (
                  <label key={schedule.id} className="flex items-center gap-2 p-1 hover:bg-charcoal-500 rounded cursor-pointer">
                    <input 
                      type="checkbox" 
                      checked={selectedSchedules.includes(schedule.id)}
                      onChange={() => toggleSchedule(schedule.id)}
                      className="w-4 h-4"
                    />
                    <span className="text-gray-200 text-sm flex-1">{schedule.name}</span>
                    <span className="text-gray-500 text-xs font-mono">{schedule.cron_expr}</span>
                  </label>
                ))
              )}
            </div>
          </div>
        </div>
        
        {table && (
          <div className="px-4 py-2 bg-charcoal-500 border-t border-charcoal-200">
            <div className="text-xs text-gray-400 space-y-1">
              {table.created_by && (
                <div>Created by: <span className="text-gray-300">{table.created_by}</span></div>
              )}
              {table.updated_by && (
                <div>Last updated by: <span className="text-gray-300">{table.updated_by}</span></div>
              )}
            </div>
          </div>
        )}
        
        <div className="border-t border-charcoal-200 px-4 py-3 flex gap-2 justify-end bg-charcoal-400">
          <button onClick={onClose} className="px-3 py-2 bg-charcoal-700 text-gray-200 border border-charcoal-200 rounded-md cursor-pointer hover:bg-charcoal-600">Cancel</button>
          <button onClick={handleSave} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Save</button>
        </div>
      </div>
    </div>
  );
}

