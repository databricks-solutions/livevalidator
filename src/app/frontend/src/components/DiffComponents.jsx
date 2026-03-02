import React, { useState } from 'react';
import { diffChars } from 'diff';

export const TRUNCATE_LENGTH = 50;
export const MAX_CHANGE_RATIO = 0.4;

export function stripWhitespace(str) {
  return str.replace(/\s+/g, '');
}

export function makeVisible(str) {
  return str
    .replace(/\t/g, '⇥')
    .replace(/\n/g, '↵\n')
    .replace(/\u00A0/g, ' ')
    .replace(/\u200B/g, '');
}

export function generateSqlQuery(tableName, row, columnsToUse = null) {
  const rowKeys = Object.keys(row);
  const cols = columnsToUse || rowKeys;
  
  const conditions = cols.map(col => {
    const actualKey = rowKeys.find(k => k.toLowerCase() === col.toLowerCase()) || col;
    const value = row[actualKey];
    if (value === null || value === undefined) return `${col} IS NULL`;
    if (typeof value === 'number') return `${col} = ${value}`;
    if (typeof value === 'boolean') return `${col} = ${value}`;
    const escaped = String(value).replace(/'/g, "''");
    return `${col} = '${escaped}'`;
  }).join('\n  AND ');
  
  return `SELECT * FROM ${tableName}\nWHERE ${conditions};`;
}

export function DiffHighlight({ source, target }) {
  const srcIsNull = source === null || source === undefined;
  const tgtIsNull = target === null || target === undefined;
  
  if (srcIsNull && tgtIsNull) {
    return <span className="text-gray-500 italic">null</span>;
  }
  
  if (srcIsNull || tgtIsNull) {
    return (
      <span className="whitespace-pre-wrap">
        {srcIsNull && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 italic mr-1">null</span>}
        {!srcIsNull && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 mr-1">{makeVisible(String(source))}</span>}
        {tgtIsNull ? (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5 italic">null</span>
        ) : (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(String(target))}</span>
        )}
      </span>
    );
  }
  
  const srcStr = String(source);
  const tgtStr = String(target);
  
  if (srcStr === tgtStr) {
    return <span className="whitespace-pre-wrap">{tgtStr || <span className="text-gray-500 italic">empty</span>}</span>;
  }
  
  if (srcStr === '' || tgtStr === '') {
    return (
      <span className="whitespace-pre-wrap">
        {srcStr !== '' && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 mr-1">{makeVisible(srcStr)}</span>}
        {srcStr === '' && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 italic mr-1">empty</span>}
        {tgtStr === '' ? (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5 italic">empty</span>
        ) : (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(tgtStr)}</span>
        )}
      </span>
    );
  }
  
  const srcStripped = stripWhitespace(srcStr);
  const tgtStripped = stripWhitespace(tgtStr);
  const whitespaceOnly = srcStripped === tgtStripped;
  
  const diff = diffChars(srcStr, tgtStr);
  
  if (whitespaceOnly) {
    return (
      <span className="whitespace-pre-wrap">
        {diff.map((part, i) => {
          if (part.added) return <span key={i} className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
          if (part.removed) return <span key={i} className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
          return <span key={i}>{part.value}</span>;
        })}
      </span>
    );
  }
  
  let changedChars = 0;
  for (const part of diff) {
    if (part.added || part.removed) changedChars += stripWhitespace(part.value).length;
  }
  const totalNonWsChars = srcStripped.length + tgtStripped.length;
  const changeRatio = totalNonWsChars > 0 ? changedChars / totalNonWsChars : 1;
  
  if (changeRatio > MAX_CHANGE_RATIO) {
    return (
      <span className="whitespace-pre-wrap bg-yellow-800/50 text-yellow-100 rounded-sm px-0.5 cursor-help" title={`Source: ${srcStr}`}>
        {tgtStr}
      </span>
    );
  }
  
  return (
    <span className="whitespace-pre-wrap">
      {diff.map((part, i) => {
        if (part.added) return <span key={i} className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
        if (part.removed) return <span key={i} className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
        return <span key={i}>{part.value}</span>;
      })}
    </span>
  );
}

export function CopySqlButton({ tableName, row, columnsToUse = null }) {
  const [copied, setCopied] = useState(false);
  
  const handleCopy = async (e) => {
    e.stopPropagation();
    const sql = generateSqlQuery(tableName, row, columnsToUse);
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };
  
  return (
    <button
      onClick={handleCopy}
      className={`text-xs py-0.5 rounded border transition-all w-[3.25rem] text-center ${
        copied 
          ? 'bg-green-900/50 border-green-600 text-green-300' 
          : 'bg-charcoal-600 border-charcoal-300 text-gray-400 hover:text-gray-200 hover:border-gray-400'
      }`}
      title="Copy SELECT query to clipboard"
    >
      {copied ? 'Copied' : 'SQL'}
    </button>
  );
}

export function ExpandableCell({ value, className = '' }) {
  const [expanded, setExpanded] = useState(false);
  
  if (value === null || value === undefined) {
    return <span className="text-gray-500 italic">null</span>;
  }
  
  const strValue = String(value);
  const needsTruncation = strValue.length > TRUNCATE_LENGTH;
  
  if (!needsTruncation) {
    return <span className={`whitespace-pre-wrap ${className}`}>{strValue}</span>;
  }
  
  return (
    <span 
      className={`cursor-pointer hover:bg-charcoal-300/30 rounded px-0.5 -mx-0.5 whitespace-pre-wrap ${className}`}
      onClick={(e) => { e.stopPropagation(); setExpanded(!expanded); }}
      title={expanded ? 'Click to collapse' : 'Click to expand'}
    >
      {expanded ? strValue : `${strValue.slice(0, TRUNCATE_LENGTH)}…`}
      {!expanded && <span className="text-xs text-gray-500 ml-1">+{strValue.length - TRUNCATE_LENGTH}</span>}
    </span>
  );
}
