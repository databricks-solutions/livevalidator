import React, { useState, useRef, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { getTagColors } from '../DashboardTagPane';

export function TagOverflowModal({ allTags, tagStates, onTagClick, onClose }) {
  const [search, setSearch] = useState('');
  const [pos, setPos] = useState({ x: null, y: null });
  const [size, setSize] = useState({ w: 420, h: 480 });
  const dragging = useRef(false);
  const resizing = useRef(false);
  const offset = useRef({ x: 0, y: 0 });
  const panelRef = useRef(null);

  useEffect(() => {
    if (pos.x === null) {
      setPos({
        x: Math.round((window.innerWidth - size.w) / 2),
        y: Math.round((window.innerHeight - size.h) / 2),
      });
    }
  }, []);

  // Close on click outside the panel
  useEffect(() => {
    const onMouseDown = (e) => {
      if (panelRef.current && !panelRef.current.contains(e.target)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [onClose]);

  // Close on Escape
  useEffect(() => {
    const onEsc = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onEsc);
    return () => window.removeEventListener('keydown', onEsc);
  }, [onClose]);

  const onDragStart = useCallback((e) => {
    if (e.target.closest('[data-resize]')) return;
    dragging.current = true;
    offset.current = { x: e.clientX - pos.x, y: e.clientY - pos.y };
    e.preventDefault();
  }, [pos]);

  const onResizeStart = useCallback((e) => {
    resizing.current = true;
    offset.current = { x: e.clientX, y: e.clientY, w: size.w, h: size.h };
    e.preventDefault();
    e.stopPropagation();
  }, [size]);

  useEffect(() => {
    const onMove = (e) => {
      if (dragging.current) {
        setPos({ x: e.clientX - offset.current.x, y: e.clientY - offset.current.y });
      } else if (resizing.current) {
        setSize({
          w: Math.max(300, offset.current.w + (e.clientX - offset.current.x)),
          h: Math.max(250, offset.current.h + (e.clientY - offset.current.y)),
        });
      }
    };
    const onUp = () => { dragging.current = false; resizing.current = false; };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    return () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
  }, []);

  const filtered = search
    ? allTags.filter(t => t.toLowerCase().includes(search.toLowerCase()))
    : allTags;

  if (pos.x === null) return null;

  return createPortal(
    <div
      ref={panelRef}
      style={{ position: 'fixed', left: pos.x, top: pos.y, width: size.w, height: size.h, zIndex: 99999 }}
      className="flex flex-col bg-charcoal-600 border border-charcoal-200 rounded-xl shadow-2xl overflow-hidden select-none"
    >
      {/* Title bar — drag handle */}
      <div
        onMouseDown={onDragStart}
        className="flex items-center justify-between px-4 py-2.5 bg-charcoal-500 border-b border-charcoal-200 cursor-move shrink-0"
      >
        <h3 className="text-sm font-semibold text-gray-100">All Tags ({allTags.length})</h3>
        <button
          onClick={onClose}
          className="w-6 h-6 flex items-center justify-center rounded hover:bg-charcoal-400 text-gray-400 hover:text-white transition-colors text-sm"
        >
          ✕
        </button>
      </div>

      {/* Search */}
      <div className="px-3 py-2 border-b border-charcoal-300/50 shrink-0">
        <input
          type="text"
          placeholder="Search tags..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full px-3 py-1.5 bg-charcoal-700 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-purple-500"
          autoFocus
        />
      </div>

      {/* Scrollable tag grid */}
      <div className="flex-1 overflow-y-auto p-3 min-h-0">
        <div className="flex flex-wrap gap-1.5">
          {filtered.map(tag => {
            const colors = getTagColors(tag);
            const state = tagStates[tag] || 'none';
            return (
              <button
                key={tag}
                onClick={(e) => { e.stopPropagation(); onTagClick(tag); }}
                className={`px-2 py-0.5 text-xs rounded transition-all duration-150 font-medium border cursor-pointer ${
                  state === 'full'
                    ? `${colors.bg} ${colors.text} ${colors.border} shadow-sm hover:opacity-80`
                    : state === 'partial'
                    ? `${colors.text} ${colors.border} border-2 border-dashed shadow-sm hover:opacity-80`
                    : 'bg-charcoal-700/50 text-gray-500 border-charcoal-500/50 hover:bg-charcoal-600/50'
                }`}
              >
                {tag}
              </button>
            );
          })}
          {filtered.length === 0 && (
            <p className="text-gray-500 text-sm italic">No tags match "{search}"</p>
          )}
        </div>
      </div>

      {/* Resize grip */}
      <div
        data-resize="true"
        onMouseDown={onResizeStart}
        className="absolute bottom-0 right-0 w-4 h-4 cursor-nwse-resize"
        style={{ background: 'linear-gradient(135deg, transparent 50%, rgba(156,163,175,0.4) 50%)', borderBottomRightRadius: '0.75rem' }}
      />
    </div>,
    document.body
  );
}
