/** Compact text pages with contiguous source and exact line continuation. */
export const DEFAULT_READ_CHARS=12_000;
export const DEFAULT_BATCH_READ_CHARS=32_000;
export function boundReadText(text:string,path:string,offset:number,length:number,maxChars=DEFAULT_READ_CHARS){
  const status=/^(\[Reading (?:last )?(\d+) lines[^\n]*\])\n\n/.exec(text);
  const total=/\(total: (\d+) lines/.exec(status?.[1]??'');
  const totalLines=total?Number(total[1]):undefined;
  const start=offset<0&&totalLines!==undefined?Math.max(0,totalLines+offset):offset;
  const body=status?text.slice(status[0].length):text;
  const requestedLines=status?Number(status[2]):0;
  const cropped=body.length>maxChars;
  // Keep complete lines when possible; never join a distant tail onto source.
  const boundary=cropped&&status?body.lastIndexOf('\n',maxChars):-1;
  const partialLine=cropped&&boundary<0;
  const shown=cropped?body.slice(0,boundary>=0?boundary:maxChars):body;
  const returnedLines=!status?0:cropped?(partialLine?0:shown.split('\n').length):requestedLines;
  const hasMoreLines=!!status&&(cropped||(totalLines!==undefined&&start+returnedLines<totalLines));
  const next=partialLine
    ?start>=0?{tool:'read_code_file',args:{path,offset:start,length:1,maxChars},note:'A single line exceeds the text budget. Read this source line, then follow its nextCharOffset.'}
      :{tool:'read_file',args:{path,offset,length,maxChars:Math.min(128000,maxChars*2)},note:'Tail line count is unknown. Request a larger bounded view of the same tail; do not invent absolute offsets.'}
    :hasMoreLines
      ?{tool:'read_file',args:{path,offset:start+returnedLines,length,maxChars}}
      :null;
  const header=status&&cropped
    ?'[Reading '+(partialLine?'part of 1':returnedLines)+' lines from line '+start+
      (totalLines!==undefined?' (total: '+totalLines+' lines, '+Math.max(0,totalLines-start-returnedLines)+' remaining)':'')+']\n\n'
    :status?.[0]??'';
  return {text:header+shown+(cropped?'\n\n[OUTPUT TRUNCATED: compact text budget. Follow read.next; source is incomplete.]':''),
    read:{path,offset:start,returnedLines,hasMoreLines,characterTruncated:partialLine,
      outputTruncated:cropped,truncated:cropped||hasMoreLines,next,
      complete:!cropped&&!hasMoreLines&&!!status,maxCharsApplied:maxChars}};
}
