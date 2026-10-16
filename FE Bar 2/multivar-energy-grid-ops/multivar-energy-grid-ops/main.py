"""Multivar Energy — Grid Operations Portal (React + FastAPI)
Queries Unity Catalog Gold tables via Databricks SQL Statement Execution API.
"""
import logging, os, time as _time
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("grid-ops")

app = FastAPI(title="Multivar Energy Grid Ops")
w = WorkspaceClient()

CATALOG = "saifur_fevm_aws_workspace_catalog"
SCHEMA = "multivar_energy"
_wh_id: str | None = None


def warehouse_id() -> str:
    global _wh_id
    if _wh_id:
        return _wh_id
    env_id = os.environ.get("SQL_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if env_id:
        _wh_id = env_id
        log.info(f"Using warehouse from env: {env_id}")
        return env_id
    try:
        for wh in w.warehouses.list():
            state = str(getattr(wh, 'state', '')).upper()
            if 'RUNNING' in state:
                _wh_id = wh.id
                log.info(f"Using warehouse {wh.name} ({wh.id})")
                return wh.id
        all_wh = list(w.warehouses.list())
        if all_wh:
            _wh_id = all_wh[0].id
            return _wh_id
    except Exception as e:
        log.warning(f"Could not list warehouses: {e}")
    raise RuntimeError("No SQL warehouse available — set SQL_WAREHOUSE_ID env var")


def query(sql: str) -> dict:
    try:
        r = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id(),
            statement=sql,
            catalog=CATALOG,
            schema=SCHEMA,
            wait_timeout="50s",
        )
        state = str(getattr(r.status, 'state', '')).upper()
        if 'SUCCEEDED' not in state:
            return {"columns": [], "rows": [], "error": f"Query state: {state}"}
        cols = [c.name for c in r.manifest.schema.columns]
        rows = []
        if r.result and r.result.data_array:
            for row in r.result.data_array:
                rows.append({cols[i]: row[i] for i in range(len(cols))})
        return {"columns": cols, "rows": rows}
    except Exception as e:
        log.exception("query error")
        return {"columns": [], "rows": [], "error": str(e)}


# ── API Endpoints ────────────────────────────────────────────────────

@app.get("/api/executive")
def api_executive():
    return query("SELECT * FROM gold_executive_kpis ORDER BY total_assets DESC")

@app.get("/api/health")
def api_health(risk: str = Query("All")):
    where = f"WHERE risk_classification = '{risk}'" if risk != "All" else ""
    return query(
        f"SELECT unified_asset_id, country, region, asset_category, voltage_level, "
        f"criticality_tier, ROUND(asset_age_years,1) AS age_years, "
        f"ROUND(health_score,1) AS health_score, risk_classification "
        f"FROM gold_asset_health_scores {where} ORDER BY health_score ASC LIMIT 200"
    )

@app.get("/api/maintenance")
def api_maintenance():
    return query(
        "SELECT unified_asset_id, country, region, asset_category, voltage_level, "
        "criticality_tier, ROUND(priority_score,2) AS priority_score, "
        "ROUND(failure_probability,4) AS failure_prob, "
        "ROUND(asset_age_years,1) AS age_years "
        "FROM gold_maintenance_priority ORDER BY priority_score DESC LIMIT 50"
    )

@app.get("/api/predictions")
def api_predictions(country: str = Query("All"), tier: str = Query("All")):
    conds = []
    if country != "All": conds.append(f"country = '{country}'")
    if tier != "All": conds.append(f"criticality_tier = '{tier}'")
    where = "WHERE " + " AND ".join(conds) if conds else ""
    return query(
        f"SELECT unified_asset_id, country, asset_category, voltage_level, "
        f"criticality_tier, ROUND(asset_age_years,1) AS age_years, "
        f"ROUND(health_score,1) AS health_score, "
        f"ROUND(ml_failure_probability,4) AS failure_prob, "
        f"ROUND(ml_priority_score,2) AS priority_score, ml_risk_label "
        f"FROM gold_ml_failure_predictions {where} "
        f"ORDER BY ml_priority_score DESC LIMIT 100"
    )


@app.get("/api/asset-detail")
def api_asset_detail(id: str = Query(...)):
    return query(f"SELECT * FROM gold_asset_health_scores WHERE unified_asset_id = '{id}'")

@app.get("/api/financial-impact")
def api_financial_impact(id: str = Query(...)):
    r = query(
        f"SELECT unified_asset_id, country, asset_category, voltage_level, "
        f"criticality_tier, ROUND(health_score,1) AS health_score, risk_classification, "
        f"ROUND(total_maintenance_cost_eur,2) AS maint_cost, "
        f"rated_capacity_mw, ROUND(asset_age_years,1) AS age_years "
        f"FROM gold_asset_health_scores WHERE unified_asset_id = '{id}'"
    )
    if r.get("rows"):
        row = r["rows"][0]
        tm = {"TIER_1_CRITICAL":5,"TIER_2_HIGH":3,"TIER_3_MEDIUM":1.5,"TIER_4_STANDARD":1}
        vm = {"HV":4,"MV":2,"LV":1}
        hs = float(row.get("health_score") or 50)
        rf = (100 - hs) / 100
        cap = max(float(row.get("rated_capacity_mw") or 1), 0.5)
        base = 75000 * tm.get(row.get("criticality_tier",""),1) * vm.get(row.get("voltage_level",""),1) * rf * cap
        r["impact"] = {"outage_cost":round(base),"revenue_risk":round(base*2.5),"regulatory_risk":round(base*.3),"total":round(base*3.8),"urgency":"IMMEDIATE" if rf>.7 else "URGENT" if rf>.4 else "SCHEDULED"}
    return r


GENIE_SPACES = {
    "grid-ops": "01f1bad41f991a4fadd4fee40820a20a",
    "executive": "01f1bad422471cddaa1ede0a5a565f63",
    "asset-health": "01f1bad4262914a9b04f8045c1a60e6c",
}

@app.get("/api/map-assets")
def api_map_assets():
    return query(
        "SELECT unified_asset_id, country, region, asset_category, voltage_level, "
        "criticality_tier, ROUND(health_score,1) AS health_score, risk_classification, "
        "latitude, longitude "
        "FROM gold_asset_health_scores "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "LIMIT 2000"
    )

@app.post("/api/genie/ask")
async def genie_ask(request: Request):
    body = await request.json()
    question = body.get("question", "")
    space_key = body.get("space", "grid-ops")
    conversation_id = body.get("conversation_id")
    space_id = GENIE_SPACES.get(space_key, GENIE_SPACES["grid-ops"])
    try:
        try:
            if conversation_id:
                resp = w.genie.create_message_and_wait(space_id=space_id, conversation_id=conversation_id, content=question)
                conv_id = conversation_id
            else:
                resp = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
                conv_id = getattr(resp, 'conversation_id', None)
        except AttributeError:
            if conversation_id:
                init = w.genie.create_message(space_id=space_id, conversation_id=conversation_id, content=question)
                conv_id = conversation_id
                msg_id = getattr(init, 'id', None) or getattr(init, 'message_id', None)
            else:
                init = w.genie.start_conversation(space_id=space_id, content=question)
                conv_id = getattr(init, 'conversation_id', None)
                msg_id = getattr(init, 'message_id', None) or getattr(init, 'id', None)
            resp = None
            for _ in range(30):
                _time.sleep(2)
                resp = w.genie.get_message(space_id=space_id, conversation_id=conv_id, message_id=msg_id)
                st = str(getattr(getattr(resp, 'status', ''), 'value', str(getattr(resp, 'status', '')))).upper()
                if 'COMPLETED' in st or 'FAILED' in st:
                    break
        text_parts, sql_text = [], ""
        for att in (getattr(resp, 'attachments', None) or []):
            t = getattr(att, 'text', None)
            if t: text_parts.append(getattr(t, 'content', str(t)))
            q = getattr(att, 'query', None)
            if q:
                sql_text = getattr(q, 'query', '') or ''
                desc = getattr(q, 'description', '') or ''
                if desc: text_parts.append(desc)
        if not text_parts:
            text_parts.append(getattr(resp, 'content', '') or 'Response received.')
        return {"conversation_id": conv_id, "text": "\n".join(str(p) for p in text_parts).strip(), "sql": sql_text}
    except Exception as e:
        log.exception("genie error")
        return {"error": str(e), "text": f"Error: {e}"}


# ── React Frontend ───────────────────────────────────────────────────

HTML = r"""
<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Multivar Energy Grid Ops</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>tailwind.config={darkMode:'class',theme:{extend:{colors:{surface:'#111827',card:'#1a2332',accent:'#10B981',accentDark:'#059669',warn:'#F59E0B',danger:'#EF4444',info:'#3B82F6'}}}}</script>
<script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<link rel="stylesheet" href="https://js.arcgis.com/4.28/esri/themes/dark/main.css">
<script src="https://js.arcgis.com/4.28/"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{background:#060b14;color:#c9d1d9;font-family:'Inter',system-ui,sans-serif}
  ::-webkit-scrollbar{width:6px;height:6px}
  ::-webkit-scrollbar-track{background:#0d1117}
  ::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
  .glow{box-shadow:0 0 20px rgba(16,185,129,0.08)}
  @keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
  .animate-in{animation:fadeIn .4s ease-out}
  @keyframes pulse-subtle{0%,100%{opacity:1}50%{opacity:.6}}
  .loading-pulse{animation:pulse-subtle 1.5s infinite}
  .esri-view .esri-popup__main-container{background:#1a2332!important;color:#c9d1d9!important;border:1px solid rgba(55,65,81,0.6)!important}
  .esri-popup__header-title{color:#10B981!important}
  .esri-widget{background:#111827!important;color:#c9d1d9!important}
  .esri-popup__action-text{color:#10B981!important}
  .modal-overlay{position:fixed;inset:0;z-index:50;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.65);backdrop-filter:blur(4px)}
  .floating-chat{position:fixed;bottom:88px;right:24px;width:420px;height:580px;z-index:45}
  @media(max-width:640px){.floating-chat{width:calc(100% - 32px);right:16px;bottom:80px;height:70vh}}
</style>
</head><body>
<div id="root"></div>
<script type="text/babel">
const {useState,useEffect,useCallback} = React;

const TABS = [
  {id:'executive',label:'Executive Overview',icon:String.fromCodePoint(0x1F4CA)},
  {id:'health',label:'Asset Health',icon:String.fromCodePoint(0x1F3E5)},
  {id:'maintenance',label:'Maintenance Queue',icon:String.fromCodePoint(0x1F527)},
  {id:'predictions',label:'ML Predictions',icon:String.fromCodePoint(0x1F916)},
  {id:'map',label:'Grid Map',icon:String.fromCodePoint(0x1F30D)},
];

function Spinner(){return <div className="flex justify-center py-16"><div className="loading-pulse text-accent text-lg font-medium">Loading data...</div></div>}
function Badge({text,color}){const c={CRITICAL:'bg-red-500/20 text-red-400 border-red-500/30',HIGH_RISK:'bg-orange-500/20 text-orange-400 border-orange-500/30',MODERATE:'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',HEALTHY:'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',TIER_1_CRITICAL:'bg-red-500/20 text-red-400 border-red-500/30',TIER_2_HIGH:'bg-orange-500/20 text-orange-400 border-orange-500/30',TIER_3_MEDIUM:'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',TIER_4_STANDARD:'bg-slate-500/20 text-slate-400 border-slate-500/30'};return <span className={`px-2 py-0.5 rounded-full text-[11px] font-semibold border ${c[text]||'bg-slate-700 text-slate-300 border-slate-600'}`}>{text}</span>}

function KPI({title,value,sub,color='text-accent'}){return(
  <div className="bg-card border border-gray-800/60 rounded-xl p-5 glow animate-in">
    <p className="text-[11px] text-gray-500 uppercase tracking-widest font-semibold mb-2">{title}</p>
    <p className={`text-3xl font-extrabold ${color}`}>{value||'--'}</p>
    {sub&&<p className="text-xs text-gray-500 mt-1">{sub}</p>}
  </div>
)}

function Table({columns,rows,badges=[],onAssetClick,actions}){if(!rows||!rows.length)return <p className="text-gray-600 py-8 text-center">No data</p>;return(
  <div className="overflow-x-auto rounded-xl border border-gray-800/60 animate-in">
    <table className="w-full text-sm">
      <thead><tr className="bg-gray-900/80">{columns.map(c=><th key={c} className="px-4 py-3 text-left text-[11px] text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-800">{c.replace(/_/g,' ')}</th>)}{actions&&<th className="px-4 py-3 text-left text-[11px] text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-800">Actions</th>}</tr></thead>
      <tbody>{rows.map((r,i)=><tr key={i} className="border-b border-gray-800/40 hover:bg-gray-800/30 transition-colors">{columns.map(c=><td key={c} className="px-4 py-2.5 text-gray-300 whitespace-nowrap">{c==='unified_asset_id'&&onAssetClick?<button onClick={()=>onAssetClick(r[c])} className="text-accent hover:underline cursor-pointer font-medium">{r[c]}</button>:badges.includes(c)?<Badge text={r[c]||''}/>:r[c]}</td>)}{actions&&<td className="px-4 py-2.5">{actions(r)}</td>}</tr>)}</tbody>
    </table>
  </div>
)}

function Select({label,value,options,onChange}){return(
  <div className="flex flex-col gap-1">
    <label className="text-[11px] text-gray-500 uppercase tracking-wider font-semibold">{label}</label>
    <select value={value} onChange={e=>onChange(e.target.value)} className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent/30">
      {options.map(o=><option key={o} value={o}>{o}</option>)}
    </select>
  </div>
)}

function ExecutiveTab({data,loading}){if(loading)return <Spinner/>;
  const rows=data?.rows||[];const sum=(f)=>rows.reduce((a,r)=>a+(parseFloat(r[f])||0),0);
  const avg=(f)=>{const vals=rows.map(r=>parseFloat(r[f])).filter(v=>!isNaN(v));return vals.length?(vals.reduce((a,b)=>a+b,0)/vals.length).toFixed(1):0};
  return(<div className="space-y-6">
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <KPI title="Total Grid Assets" value={sum('total_assets').toLocaleString()} sub="Across 14 countries"/>
      <KPI title="Avg Health Score" value={avg('avg_health_score')} color="text-blue-400" sub="0-100 scale"/>
      <KPI title="Avg SAIDI" value={`${avg('avg_saidi_minutes')} min`} color="text-yellow-400" sub="System avg interruption"/>
      <KPI title="Renewable Generation" value={`${Math.round(sum('total_renewable_generation_mw')).toLocaleString()} MW`} color="text-emerald-400" sub="Total DER output"/>
    </div>
    <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider">KPIs by Country</h3>
    <Table columns={['country','total_assets','avg_health_score','avg_saifi','avg_saidi_minutes','total_renewable_generation_mw','total_major_events']} rows={rows}/>
  </div>)
}

function HealthTab({data,loading,onFilter,onAssetClick}){const[risk,setRisk]=useState('All');
  useEffect(()=>{onFilter(risk)},[risk]);
  if(loading)return <Spinner/>;
  return(<div className="space-y-4">
    <div className="flex gap-4 items-end"><Select label="Risk Classification" value={risk} options={['All','CRITICAL','HIGH_RISK','MODERATE','HEALTHY']} onChange={setRisk}/></div>
    <Table columns={['unified_asset_id','country','region','asset_category','voltage_level','criticality_tier','age_years','health_score','risk_classification']} rows={data?.rows||[]} badges={['risk_classification','criticality_tier']} onAssetClick={onAssetClick}/>
  </div>)
}

function MaintenanceTab({data,loading,onAssetClick,onWorkOrder}){if(loading)return <Spinner/>;return(
  <div className="space-y-4">
    <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
      <KPI title="Queue Size" value={data?.rows?.length||0} color="text-orange-400"/>
      <KPI title="Highest Priority" value={data?.rows?.[0]?.priority_score||'--'} color="text-red-400"/>
      <KPI title="Countries Affected" value={[...new Set((data?.rows||[]).map(r=>r.country))].length} color="text-blue-400"/>
    </div>
    <Table columns={['unified_asset_id','country','region','asset_category','voltage_level','criticality_tier','priority_score','failure_prob','age_years']} rows={data?.rows||[]} badges={['criticality_tier']} onAssetClick={onAssetClick} actions={(r)=><button onClick={()=>onWorkOrder(r)} className="px-2 py-1 bg-orange-500/20 text-orange-400 border border-orange-500/30 rounded-lg text-[10px] font-semibold hover:bg-orange-500/30">Create WO</button>}/>
  </div>)
}

function PredictionsTab({data,loading,onFilter,onAssetClick}){const[country,setCountry]=useState('All');const[tier,setTier]=useState('All');
  useEffect(()=>{onFilter(country,tier)},[country,tier]);
  if(loading)return <Spinner/>;
  const rows=data?.rows||[];const highRisk=rows.filter(r=>r.ml_risk_label==='HIGH_RISK').length;
  return(<div className="space-y-4">
    <div className="flex gap-4 items-end flex-wrap">
      <Select label="Country" value={country} options={['All','Germany','France','Netherlands','Spain','Italy','Belgium','Austria','Denmark','Poland','Sweden','Norway','Switzerland','Portugal','Ireland']} onChange={setCountry}/>
      <Select label="Criticality Tier" value={tier} options={['All','TIER_1_CRITICAL','TIER_2_HIGH','TIER_3_MEDIUM','TIER_4_STANDARD']} onChange={setTier}/>
      <div className="ml-auto"><KPI title="High Risk Assets" value={highRisk} color="text-red-400"/></div>
    </div>
    <Table columns={['unified_asset_id','country','asset_category','voltage_level','criticality_tier','age_years','health_score','failure_prob','priority_score','ml_risk_label']} rows={rows} badges={['criticality_tier','ml_risk_label']} onAssetClick={onAssetClick}/>
  </div>)
}

function MapTab({data,loading}){const mapRef=React.useRef(null);const[ready,setReady]=useState(false);
  useEffect(()=>{
    if(!data?.rows?.length||ready)return;
    require(["esri/Map","esri/views/MapView","esri/layers/GraphicsLayer","esri/Graphic"],function(Map,MapView,GraphicsLayer,Graphic){
      const gl=new GraphicsLayer();
      const map=new Map({basemap:"dark-gray-vector",layers:[gl]});
      new MapView({container:mapRef.current,map,center:[10,50],zoom:4,popup:{dockEnabled:true,dockOptions:{position:"top-right"}}});
      const rc={CRITICAL:[239,68,68,.9],HIGH_RISK:[249,115,22,.9],MODERATE:[234,179,8,.8],HEALTHY:[16,185,129,.7]};
      data.rows.forEach(r=>{const lat=parseFloat(r.latitude),lon=parseFloat(r.longitude);if(isNaN(lat)||isNaN(lon))return;
        gl.add(new Graphic({geometry:{type:"point",latitude:lat,longitude:lon},symbol:{type:"simple-marker",color:rc[r.risk_classification]||[128,128,128,.6],size:r.risk_classification==='CRITICAL'?11:r.risk_classification==='HIGH_RISK'?9:6,outline:{color:[255,255,255,.4],width:.5}},attributes:r,
          popupTemplate:{title:`${r.asset_category} \u2014 ${r.unified_asset_id}`,content:`<div style="color:#c9d1d9;font-size:13px"><b>Country:</b> ${r.country} (${r.region})<br><b>Health Score:</b> ${r.health_score}/100<br><b>Risk:</b> ${r.risk_classification}<br><b>Tier:</b> ${r.criticality_tier}<br><b>Voltage:</b> ${r.voltage_level}</div>`}}))});
      setReady(true);});
  },[data]);
  if(loading)return <Spinner/>;
  const rows=data?.rows||[];const cnt={CRITICAL:0,HIGH_RISK:0,MODERATE:0,HEALTHY:0};rows.forEach(r=>{if(cnt[r.risk_classification]!==undefined)cnt[r.risk_classification]++});
  return(<div className="space-y-4 animate-in">
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4"><KPI title="Critical" value={cnt.CRITICAL} color="text-red-400"/><KPI title="High Risk" value={cnt.HIGH_RISK} color="text-orange-400"/><KPI title="Moderate" value={cnt.MODERATE} color="text-yellow-400"/><KPI title="Healthy" value={cnt.HEALTHY} color="text-emerald-400"/></div>
    <div ref={mapRef} style={{width:'100%',height:'560px',borderRadius:'12px',border:'1px solid rgba(55,65,81,0.4)'}}></div>
    <div className="flex gap-6 text-xs text-gray-500 justify-center"><span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-red-500"></span>Critical</span><span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-orange-500"></span>High Risk</span><span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-yellow-500"></span>Moderate</span><span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-emerald-500"></span>Healthy</span></div>
  </div>)
}

function ChatPanel({msgs,loading,onSend,space,setSpace,onNewChat,currentTab}){const[input,setInput]=useState('');const endRef=React.useRef(null);
  useEffect(()=>{endRef.current?.scrollIntoView({behavior:'smooth'})},[msgs]);
  const send=()=>{if(!input.trim()||loading)return;onSend(input.trim());setInput('')};
  const allQ={executive:["What countries have the highest total assets?","Compare renewable generation by country","Which country has the worst SAIDI?","Show KPIs ranked by health score"],health:["List critical assets with health score below 30","What asset types have highest anomaly rates?","Show health score by manufacturer","Which regions have most at-risk assets?"],maintenance:["Total maintenance cost for critical assets?","Priority score distribution by asset type","Which DSOs have most emergency repairs?","Top 10 most expensive maintenance assets"],predictions:["Highest ML failure probability assets?","Compare ML predictions across tiers","How many assets predicted HIGH_RISK?","ML priority scores by country"],map:["Regions with most critical assets?","Asset count by country and voltage","All assets in Germany with health below 40","Compare asset age across countries"]};
  const suggestions=allQ[currentTab]||allQ.executive;
  return(
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-800/60 bg-[#0d1117]">
        <div className="flex items-center gap-2"><span className="text-accent text-sm">{String.fromCodePoint(0x1F916)}</span>
          <select value={space} onChange={e=>setSpace(e.target.value)} className="bg-transparent border-0 text-[11px] text-gray-400 focus:outline-none cursor-pointer">{['grid-ops','executive','asset-health'].map(o=><option key={o} value={o}>{o}</option>)}</select></div>
        <button onClick={onNewChat} className="text-[10px] text-gray-500 hover:text-accent px-2 py-1 rounded hover:bg-gray-800/80 transition-colors">+ New Chat</button>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-2.5">
        {msgs.length===0&&<div className="text-center py-6"><p className="text-xl mb-1">{String.fromCodePoint(0x1F916)}</p><p className="text-gray-500 text-[11px] mb-4">Ask about grid operations</p>
          <div className="flex flex-wrap gap-1.5 justify-center">{suggestions.map((s,i)=><button key={i} onClick={()=>onSend(s)} className="px-2.5 py-1 bg-gray-800/80 border border-gray-700 rounded-lg text-[10px] text-gray-400 hover:border-accent hover:text-accent transition-all text-left">{s}</button>)}</div></div>}
        {msgs.map((m,i)=><div key={i} className={`flex ${m.role==='user'?'justify-end':'justify-start'}`}>
          <div className={`max-w-[85%] px-3 py-2 rounded-xl text-xs ${m.role==='user'?'bg-accent/20 text-emerald-300 border border-accent/30':'bg-gray-800/80 text-gray-200 border border-gray-700/60'}`}>
            {m.sql&&<pre className="text-[9px] text-blue-400 bg-gray-900 rounded p-1.5 mb-1.5 overflow-x-auto font-mono">{m.sql}</pre>}
            <p className="whitespace-pre-wrap">{m.text}</p></div></div>)}
        {loading&&<div className="flex justify-start"><div className="px-3 py-2 rounded-xl bg-gray-800/80 border border-gray-700/60 loading-pulse text-xs text-gray-400">Analyzing with Genie...</div></div>}
        <div ref={endRef}/></div>
      <div className="border-t border-gray-800 p-2 flex gap-1.5">
        <input value={input} onChange={e=>setInput(e.target.value)} onKeyDown={e=>e.key==='Enter'&&send()} placeholder="Ask a question..." disabled={loading}
          className="flex-1 bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-xs text-gray-200 focus:border-accent focus:outline-none disabled:opacity-50"/>
        <button onClick={send} disabled={loading||!input.trim()} className="px-3 py-2 bg-accent rounded-lg text-xs font-semibold text-white hover:bg-accentDark transition-colors disabled:opacity-50">Send</button>
      </div>
    </div>)
}

function Modal({open,onClose,title,wide,children}){if(!open)return null;return(
  <div className="modal-overlay" onClick={onClose}><div className={`bg-[#111827] border border-gray-800 rounded-2xl ${wide?'max-w-4xl':'max-w-2xl'} w-full max-h-[85vh] overflow-y-auto mx-4 shadow-2xl animate-in`} onClick={e=>e.stopPropagation()}>
    <div className="flex items-center justify-between p-5 border-b border-gray-800/60"><h2 className="text-lg font-bold text-white">{title}</h2>
      <button onClick={onClose} className="text-gray-500 hover:text-white text-xl w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-800">&times;</button></div>
    <div className="p-5">{children}</div></div></div>)}

function AssetDetailModal({assetId,onClose}){const[d,setD]=useState(null);const[ld,setLd]=useState(true);
  useEffect(()=>{if(!assetId)return;setLd(true);fetch(`/api/asset-detail?id=${encodeURIComponent(assetId)}`).then(r=>r.json()).then(j=>{setD(j.rows?.[0]);setLd(false)}).catch(()=>setLd(false))},[assetId]);
  if(!assetId)return null;
  return(<Modal open={!!assetId} onClose={onClose} title={`Asset Details \u2014 ${assetId}`} wide>
    {ld?<Spinner/>:!d?<p className="text-gray-500">Not found</p>:
    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">{Object.entries(d).filter(([k])=>!['scored_at'].includes(k)).map(([k,v])=>
      <div key={k} className="bg-gray-900/60 rounded-lg p-3"><p className="text-[10px] text-gray-500 uppercase tracking-wider">{k.replace(/_/g,' ')}</p>
        <p className="text-sm text-gray-200 mt-0.5 font-medium">{['risk_classification','criticality_tier'].includes(k)?<Badge text={v||''}/>:v||'\u2014'}</p></div>)}</div>}
  </Modal>)}

function WorkOrderModal({asset,onClose}){const[step,setStep]=useState(0);const[impact,setImpact]=useState(null);const[form,setForm]=useState({type:'EMERGENCY_REPAIR',team:'',notes:''});
  const steps=['Request Submitted','Work Order Created','Accepted','Field Ops Dispatched'];
  useEffect(()=>{if(!asset?.unified_asset_id)return;setStep(0);setImpact(null);fetch(`/api/financial-impact?id=${encodeURIComponent(asset.unified_asset_id)}`).then(r=>r.json()).then(j=>setImpact(j.impact)).catch(()=>{})},[asset]);
  const advance=()=>{if(step<3)setStep(s=>s+1)};
  if(!asset)return null;
  const fmt=(n)=>n?`\u20AC${Number(n).toLocaleString()}`:'\u2014';
  return(<Modal open={!!asset} onClose={onClose} title="Create Work Order" wide>
    <div className="space-y-5">
      <div className="flex gap-4 items-center bg-gray-900/60 rounded-xl p-4"><div className="flex-1"><p className="text-xs text-gray-400">{asset.asset_category} \u2014 {asset.unified_asset_id}</p><p className="text-base font-bold text-white">{asset.country} ({asset.region})</p></div><Badge text={asset.criticality_tier||''}/></div>
      <div className="flex items-center gap-1 overflow-x-auto pb-2">{steps.map((s,i)=><React.Fragment key={i}>
        <div className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[10px] font-semibold whitespace-nowrap ${i<=step?'bg-accent/20 text-accent border border-accent/30':'bg-gray-800 text-gray-500 border border-gray-700'}`}>
          <span className={`w-4 h-4 rounded-full flex items-center justify-center text-[9px] ${i<=step?'bg-accent text-white':'bg-gray-700 text-gray-400'}`}>{i<step?'\u2713':i+1}</span>{s}</div>
        {i<3&&<span className={`text-[10px] ${i<step?'text-accent':'text-gray-700'}`}>\u2192</span>}
      </React.Fragment>)}</div>
      {impact&&<div className="bg-red-500/5 border border-red-500/20 rounded-xl p-4"><h3 className="text-xs font-bold text-red-400 mb-3">\u26a0 Financial Impact if Not Addressed</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div><p className="text-[10px] text-gray-500 uppercase">Outage Cost</p><p className="text-lg font-bold text-red-400">{fmt(impact.outage_cost)}</p></div>
          <div><p className="text-[10px] text-gray-500 uppercase">Revenue at Risk</p><p className="text-lg font-bold text-orange-400">{fmt(impact.revenue_risk)}</p></div>
          <div><p className="text-[10px] text-gray-500 uppercase">Regulatory</p><p className="text-lg font-bold text-yellow-400">{fmt(impact.regulatory_risk)}</p></div>
          <div><p className="text-[10px] text-gray-500 uppercase">Total Impact</p><p className="text-lg font-bold text-red-300">{fmt(impact.total)}</p></div>
        </div>
        <p className="text-[10px] text-gray-500 mt-2">Urgency: <span className={`font-bold ${impact.urgency==='IMMEDIATE'?'text-red-400':impact.urgency==='URGENT'?'text-orange-400':'text-yellow-400'}`}>{impact.urgency}</span></p></div>}
      {step===0&&<div className="space-y-3">
        <Select label="Work Type" value={form.type} options={['EMERGENCY_REPAIR','PREVENTIVE_MAINTENANCE','CONDITION_BASED','INSPECTION']} onChange={v=>setForm(f=>({...f,type:v}))}/>
        <Select label="Assigned Team" value={form.team||''} options={['','Alpha Crew','Bravo Crew','Charlie Crew','Delta Crew','External Contractor']} onChange={v=>setForm(f=>({...f,team:v}))}/>
        <div className="flex flex-col gap-1"><label className="text-[11px] text-gray-500 uppercase tracking-wider font-semibold">Notes</label>
          <textarea value={form.notes} onChange={e=>setForm(f=>({...f,notes:e.target.value}))} rows={2} className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 focus:border-accent focus:outline-none"/></div></div>}
      <button onClick={advance} disabled={step>=3} className="w-full py-2.5 bg-accent rounded-lg text-sm font-semibold text-white hover:bg-accentDark transition-colors disabled:opacity-50">{step===0?'Submit Request':step===1?'Create Work Order':step===2?'Accept & Assign':'Dispatched \u2713'}</button>
    </div>
  </Modal>)}

function App(){
  const[tab,setTab]=useState('executive');
  const[data,setData]=useState({});
  const[loading,setLoading]=useState({});
  const[chatMsgs,setChatMsgs]=useState([]);
  const[genieConvId,setGenieConvId]=useState(null);
  const[chatLoading,setChatLoading]=useState(false);
  const[selectedSpace,setSelectedSpace]=useState('grid-ops');
  const[selectedAsset,setSelectedAsset]=useState(null);
  const[workOrderAsset,setWorkOrderAsset]=useState(null);
  const[chatOpen,setChatOpen]=useState(false);
  const onNewChat=useCallback(()=>{setChatMsgs([]);setGenieConvId(null)},[]);

  const fetchData=useCallback(async(endpoint,params={})=>{
    const key=endpoint.split('/').pop();
    setLoading(l=>({...l,[key]:true}));
    try{
      const qs=Object.entries(params).filter(([,v])=>v!=='All').map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&');
      const url=qs?`/api/${key}?${qs}`:`/api/${key}`;
      const r=await fetch(url);const j=await r.json();
      setData(d=>({...d,[key]:j}));
    }catch(e){console.error(e)}
    setLoading(l=>({...l,[key]:false}));
  },[]);

  useEffect(()=>{fetchData('executive');fetchData('maintenance')},[]);
  useEffect(()=>{if(tab==='health')fetchData('health');if(tab==='predictions')fetchData('predictions');if(tab==='map'&&!data['map-assets'])fetchData('map-assets')},[tab]);

  const sendChat=useCallback(async(question)=>{
    setChatMsgs(m=>[...m,{role:'user',text:question}]);
    setChatLoading(true);
    try{
      const r=await fetch('/api/genie/ask',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question,space:selectedSpace,conversation_id:genieConvId})});
      const j=await r.json();
      if(j.conversation_id)setGenieConvId(j.conversation_id);
      setChatMsgs(m=>[...m,{role:'assistant',text:j.text||j.error||'No response',sql:j.sql}]);
    }catch(e){setChatMsgs(m=>[...m,{role:'assistant',text:'Connection error: '+e.message}])}
    setChatLoading(false);
  },[genieConvId,selectedSpace]);

  return(
    <div className="min-h-screen">
      {/* Header */}
      <header className="border-b border-gray-800/60 bg-gradient-to-r from-[#060b14] via-[#0a1628] to-[#060b14]">
        <div className="max-w-7xl mx-auto px-6 py-5 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold"><span className="text-accent">&#9889;</span> Multivar Energy <span className="text-gray-500 font-normal text-lg">Grid Operations Portal</span></h1>
            <p className="text-xs text-gray-500 mt-1">20 DSOs &middot; 14 Countries &middot; Powered by Databricks Lakehouse</p>
          </div>
          <div className="flex items-center gap-3">
            <span className="w-2 h-2 rounded-full bg-accent animate-pulse"></span>
            <span className="text-xs text-gray-500">Connected</span>
          </div>
        </div>
      </header>

      {/* Tab Nav */}
      <nav className="border-b border-gray-800/40 bg-[#0a0f1a]">
        <div className="max-w-7xl mx-auto px-6 flex gap-1">
          {TABS.map(t=>(
            <button key={t.id} onClick={()=>setTab(t.id)}
              className={`px-5 py-3 text-sm font-medium transition-all border-b-2 ${
                tab===t.id?'border-accent text-accent':'border-transparent text-gray-500 hover:text-gray-300 hover:border-gray-700'
              }`}>{t.icon} {t.label}</button>
          ))}
        </div>
      </nav>

      {/* Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {tab==='executive'&&<ExecutiveTab data={data.executive} loading={loading.executive}/>}
        {tab==='health'&&<HealthTab data={data.health} loading={loading.health} onFilter={(r)=>fetchData('health',{risk:r})} onAssetClick={setSelectedAsset}/>}
        {tab==='maintenance'&&<MaintenanceTab data={data.maintenance} loading={loading.maintenance} onAssetClick={setSelectedAsset} onWorkOrder={setWorkOrderAsset}/>}
        {tab==='predictions'&&<PredictionsTab data={data.predictions} loading={loading.predictions} onFilter={(c,t)=>fetchData('predictions',{country:c,tier:t})} onAssetClick={setSelectedAsset}/>}
        {tab==='map'&&<MapTab data={data['map-assets']} loading={loading['map-assets']}/>}
      </main>

      {/* Floating AI Assistant */}
      <button onClick={()=>setChatOpen(o=>!o)} className="fixed bottom-6 right-6 w-14 h-14 bg-accent rounded-full shadow-lg shadow-accent/25 flex items-center justify-center text-white hover:bg-accentDark transition-all z-40 text-xl" title="AI Assistant">{chatOpen?'\u00D7':String.fromCodePoint(0x1F4AC)}</button>
      {chatOpen&&<div className="floating-chat bg-[#111827] border border-gray-800 rounded-2xl shadow-2xl overflow-hidden animate-in">
        <ChatPanel msgs={chatMsgs} loading={chatLoading} onSend={sendChat} space={selectedSpace} setSpace={(s)=>{setSelectedSpace(s);setGenieConvId(null);setChatMsgs([])}} onNewChat={onNewChat} currentTab={tab}/>
      </div>}

      {/* Modals */}
      <AssetDetailModal assetId={selectedAsset} onClose={()=>setSelectedAsset(null)}/>
      <WorkOrderModal asset={workOrderAsset} onClose={()=>setWorkOrderAsset(null)}/>

      {/* Footer */}
      <footer className="border-t border-gray-800/40 py-4 mt-8">
        <p className="text-center text-[11px] text-gray-600">Lakeflow &middot; Unity Catalog &middot; Lakebase &middot; MLflow &middot; Genie Agents &middot; Databricks Apps</p>
      </footer>
    </div>
  )
}
ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
</script></body></html>
"""

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML
