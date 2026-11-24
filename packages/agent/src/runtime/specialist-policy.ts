/** Deterministic specialist routing for Code Mode.
 * No planning and no extra LLM: this only classifies technical domains and
 * applies the existing permission-scoped specialist execution roles.
 */
export type SpecialistRole='explorer'|'reviewer'|'tester'|'coder';

const RULES:Array<{name:string;patterns:RegExp[]}>= [
  {name:'go',patterns:[/\.go\b/i,/\bgo\.(?:mod|sum|work)\b/i,/\bgo\s+(?:test|build|run|vet|mod|get)\b/i,/\bgoroutine\b/i,/\bimagor\b/i]},
  {name:'nestjs',patterns:[/@nestjs\//i,/\bnestjs\b/i,/\bnest-cli\b/i,/\bnest\s+(?:build|start)\b/i]},
  {name:'flutter',patterns:[/\bflutter\b/i,/\bpubspec\.ya?ml\b/i,/\.dart\b/i,/\bdart\s+(?:test|run|analyze)\b/i]},
  {name:'rust',patterns:[/\bcargo(?:\.toml)?\b/i,/\.rs\b/i,/\brustc\b/i,/\bcargo\s+(?:build|test|check|clippy|run)\b/i]},
  {name:'nextjs',patterns:[/\bnext\.config\.[cm]?[jt]s\b/i,/\bnextjs\b/i,/@next\//i,/\bnext\s+(?:build|dev|start)\b/i]},
  {name:'python',patterns:[/\.py\b/i,/\bpyproject\.toml\b/i,/\bpytest\b/i,/\bpython3?\s+/i,/\bpip3?\s+/i]},
  {name:'dotnet',patterns:[/\.csproj\b/i,/\.sln\b/i,/\.cs\b/i,/\bdotnet\s+(?:build|test|run|restore|publish)\b/i]},
  {name:'java',patterns:[/\.java\b/i,/\bgradle(?:w)?\b/i,/\bmvnw?\b/i,/\bpom\.xml\b/i]},
  {name:'cloudflare',patterns:[/\bcloudflare\b/i,/\bwrangler\b/i,/\br2bucket\b/i,/\br2\s+(?:routing|cache|bucket|object)\b/i]},
  {name:'docker',patterns:[/\bdockerfile\b/i,/\bdocker\s+(?:build|compose|run|pull)\b/i,/\bcompose\.ya?ml\b/i]},
  {name:'ios',patterns:[/\.swift\b/i,/\bxcodeproj\b/i,/\bxcodebuild\b/i,/\bpodfile\b/i]},
  {name:'android',patterns:[/\bandroidmanifest\.xml\b/i,/\bgradlew?\b/i,/\.kt\b/i,/\bkotlin\b/i]},
];

function textOf(value:unknown){
  if(typeof value==='string')return value.slice(0,100_000);
  try{return JSON.stringify(value).slice(0,100_000);}catch{return '';}
}
export function inferSpecialistDomains(value:unknown):string[]{
  const text=textOf(value);
  const hits=RULES.filter(rule=>rule.patterns.some(p=>p.test(text))).map(rule=>rule.name);
  const set=new Set(hits);
  // Flutter commonly includes Android Gradle files; treat that as one product domain.
  if(set.has('flutter'))set.delete('android');
  return [...set];
}
export function specialistRoleFor(tool:string,readOnly:boolean,args:Record<string,unknown>):SpecialistRole{
  if(readOnly)return 'explorer';
  const text=(tool+' '+textOf(args)).toLowerCase();
  if(tool==='run_project_checks'||tool==='verify_project'||
     /\b(test|tests|check|checks|lint|typecheck|analy[sz]e|verification|verify)\b/.test(text))
    return 'tester';
  return 'coder';
}
export class SpecialistRoutingPolicy{
  private sourceDomains=new Set<string>();
  private observedDomains=new Set<string>();
  private roles=new Set<string>();
  private specialties=new Set<string>();
  private _autoRuns=0;
  private _explicitRuns=0;
  private _eligibleDirectCalls=0;
  private _candidateCalls=0;
  constructor(source:string){
    for(const d of inferSpecialistDomains(source))this.sourceDomains.add(d);
  }
  domains(){return [...new Set([...this.sourceDomains,...this.observedDomains])];}
  observe(tool:string,args:Record<string,unknown>){
    const callDomains=inferSpecialistDomains({tool,args});
    for(const d of callDomains)this.observedDomains.add(d);
    return callDomains;
  }
  required(){return this.domains().length>=2||this._candidateCalls>=4;}
  autoRoute(tool:string,args:Record<string,unknown>,readOnly:boolean,
    allowed:(role:SpecialistRole)=>boolean){
    const callDomains=this.observe(tool,args);
    const role=specialistRoleFor(tool,readOnly,args);
    if(!allowed(role))return null;
    this._candidateCalls++;
    if(!this.required())return null;
    this._eligibleDirectCalls++;
    const specialty=callDomains[0]??this.domains()[0]??'general';
    this._autoRuns++;this.roles.add(role);this.specialties.add(specialty);
    return {role,specialty,reason:(this.domains().length>=2?'multi_domain_workflow':'complex_workflow') as 'multi_domain_workflow'|'complex_workflow'};
  }
  recordExplicit(role:string,specialty?:string){
    this._explicitRuns++;this.roles.add(role);
    if(specialty)this.specialties.add(specialty);
  }
  summary(){
    const domains=this.domains();
    const required=domains.length>=2||this._candidateCalls>=4;
    return {
      enabled:true,mode:'mandatory-auto-specialist-for-complex-or-multi-domain',
      planner:false,separateModel:false,required,detectedDomains:domains.slice(0,12),
      sourceDomains:[...this.sourceDomains].slice(0,12),
      observedDomains:[...this.observedDomains].slice(0,12),
      autoRoutedCalls:this._autoRuns,explicitRuns:this._explicitRuns,
      candidateCalls:this._candidateCalls,eligibleDirectCalls:this._eligibleDirectCalls,
      rolesUsed:[...this.roles],specialtiesUsed:[...this.specialties],
      compliant:!required||(this._autoRuns+this._explicitRuns)>0,
      note:required
        ?'Complex or multi-domain work is specialist-routed automatically even when the calling AI omits tools.agent.run(). Explicit specialist delegation still takes priority.'
        :'Short simple work is not forced through specialist overhead.',
    };
  }
}
export function specialistPolicyStatus(){
  return {
    enabled:true,mode:'mandatory-auto-specialist-for-complex-or-multi-domain',planner:false,separateModel:false,
    thresholdDomains:2,thresholdEligibleCalls:4,
    automaticRoles:{readOnly:'explorer',verification:'tester',mutation:'coder'},
    supportedSpecialties:RULES.map(x=>x.name),
  };
}
