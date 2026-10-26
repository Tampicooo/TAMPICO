// Tampico website build: gives the game the same shared-data and live-room API it uses inside Claude,
// backed by Supabase (database + realtime presence). Players get a private anonymous ID automatically.
(function(){
  const CFG=window.SITE_CFG;
  const KEYSTORE="tampico3:ownerKey";
  let ownerKey="";try{ownerKey=localStorage.getItem(KEYSTORE)||""}catch(e){}
  const NET=window.TAMPICO_NET={lib:window.supabase?(window.supabase.__ver||"loaded"):"missing",auth:"pending",db:"pending",live:"pending",errors:[]};
  const note=(what,e)=>{const m=what+": "+String(e&&(e.message||e.error_description||e.code)||e).slice(0,240);NET.errors.unshift(new Date().toLocaleTimeString()+" "+m);NET.errors.length=Math.min(NET.errors.length,12);
    console.warn("Tampico",m);try{if(window.__bugq&&!NET["seen:"+m]){NET["seen:"+m]=1;window.__bugq.push({k:"net",m});window.__bugq.flush&&window.__bugq.flush()}}catch(_){}};
  if(!window.supabase||!CFG){NET.auth=NET.db=NET.live="no library";window.claude={use:async()=>null};return}
  const sb=window.supabase.createClient(CFG.url,CFG.key,{auth:{persistSession:true,autoRefreshToken:true},global:{headers:ownerKey?{"x-owner-key":ownerKey}:{}}});
  window.__sb=sb;
  const ready=(async()=>{try{let {data}=await sb.auth.getSession();let s=data&&data.session;
      if(!s){const r=await sb.auth.signInAnonymously();if(r.error)throw r.error;s=r.data.session}NET.auth="ok";
      // realtime must use the signed-in player's token
      try{sb.realtime.setAuth(s.access_token)}catch(_){}
      return s.user.id}catch(e){NET.auth="failed";note("sign-in",e);return null}})();
  const err=(e,code)=>{note("database",e);NET.db="error";const o={code:code||"unavailable",message:String(e&&e.message||e)};return o};
  const okDb=()=>{if(NET.db!=="ok")NET.db="ok"};
  const clone=o=>o==null?o:JSON.parse(JSON.stringify(o));
  const snap=(id,data)=>({id,exists:data!=null,data:()=>data==null?undefined:clone(data),metadata:{fromCache:false,hasPendingWrites:false}});
  function deepMerge(a,b){const o=Object.assign({},a);for(const k in b){const v=b[k];o[k]=v&&typeof v==="object"&&!Array.isArray(v)&&o[k]&&typeof o[k]==="object"&&!Array.isArray(o[k])?deepMerge(o[k],v):v}return o}
  function split(path){const p=path.split("/");if(p.length!==2)throw new TypeError("Tampico site build supports collection/doc paths only: "+path);return p}
  let UID=null;
  function docRef(path){const [col,id]=split(path);
    const r={id,path,
      async get(){const {data,error}=await sb.from("docs").select("data").eq("path",path).maybeSingle();if(error)throw err(error);okDb();return snap(id,data?data.data:null)},
      async set(d){const {error}=await sb.from("docs").upsert({path,col,id,owner:UID,data:clone(d),updated_at:new Date().toISOString()},{onConflict:"path"});if(error)throw err(error,"invalid_argument");okDb()},
      async update(d){const cur=await r.get();if(!cur.exists)throw err("document does not exist","invalid_argument");await r.set(deepMerge(cur.data(),d))},
      async delete(){const {error}=await sb.from("docs").delete().eq("path",path);if(error)throw err(error)},
      async acquire(o){const {data,error}=await sb.rpc("acquire_lock",{lname:path,ttl_ms:Math.max(1000,Math.min(600000,(o&&o.ttlMs)||30000))});if(error)throw err(error);return{acquired:data===true}},
      onSnapshot(next,onErr){return poll(()=>r.get(),next,onErr)},
      collection(){throw new TypeError("subcollections are not used in the site build")}};
    return r}
  function colRef(col,filters,ord,lim){filters=filters||[];lim=lim||1000;
    const q={path:col,
      where:(f,op,v)=>colRef(col,filters.concat([[f,op,v]]),ord,lim),
      orderBy:(f,d)=>colRef(col,filters,[f,d||"asc"],lim),
      limit:n=>colRef(col,filters,ord,n),
      async get(){let x=sb.from("docs").select("id,data").eq("col",col);
        for(const [f,op,v] of filters){if(op==="==")x=x.eq("data->>"+f,String(v));else if(op==="!=")x=x.neq("data->>"+f,String(v))}
        if(ord)x=x.order("data->"+ord[0],{ascending:ord[1]!=="desc",nullsFirst:false});
        const {data,error}=await x.limit(lim);if(error)throw err(error);okDb();
        const docs=(data||[]).map(r=>snap(r.id,r.data));return{docs,size:docs.length,empty:!docs.length,docChanges:()=>[],metadata:{fromCache:false,hasPendingWrites:false}}},
      onSnapshot(next,onErr){return poll(()=>q.get(),next,onErr)},
      doc:id=>docRef(col+"/"+(id||Math.random().toString(36).slice(2))),
      async add(d){const r=docRef(col+"/"+Math.random().toString(36).slice(2));await r.set(d);return r}};
    return q}
  // live views refresh every 20 seconds (and right away when the tab comes back)
  function poll(fn,next,onErr){let stop=false,t=null;const run=()=>{if(stop)return;fn().then(v=>{if(!stop)next(v)},e=>{if(onErr&&e.code!=="unavailable"){onErr(e);stop=true}}).finally(()=>{if(!stop)t=setTimeout(run,10000)})};
    const vis=()=>{if(!document.hidden&&!stop){clearTimeout(t);run()}};document.addEventListener("visibilitychange",vis);run();
    return()=>{stop=true;clearTimeout(t);document.removeEventListener("visibilitychange",vis)}}
  const DB={doc:docRef,collection:p=>colRef(p)};
  const USER={id:async()=>UID,me:async()=>({id:UID,name:""}),isOwner:()=>!!ownerKey,canEdit:()=>!!ownerKey,can:()=>true,profiles:async()=>({})};

  // ---- live rooms on Supabase Realtime presence ----
  const PEER=Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-6);
  function mkRoom(name){const ch=sb.channel("tampico-"+name,{config:{presence:{key:PEER}}});
    let mine={},peers=Object.freeze([]),conn=false,pend=null,lastSent=0;const hs=new Set(),cs=new Set();
    const me=()=>({peer:PEER,by:UID,isMe:true,sameTab:true,kind:"viewer",guest:false,presence:Object.freeze(clone(mine)),updatedAt:Date.now()});
    function rebuild(){const st=ch.presenceState(),out=[];for(const k in st){if(k===PEER)continue;const m=st[k][st[k].length-1]||{};out.push({peer:k,by:m.by||null,isMe:false,sameTab:false,kind:"viewer",guest:false,presence:Object.freeze(m.p||{}),updatedAt:Date.now()})}
      out.unshift(me());peers=Object.freeze(out);fireSoon()}
    // like the platform: listeners hear about changes at most once per tick, never re-entrantly
    let fq=false;function fireSoon(){if(fq)return;fq=true;setTimeout(()=>{fq=false;const chg={peers,joined:[],left:[],updated:[]};hs.forEach(h=>{try{h(chg)}catch(e){console.error(e)}})},16)}
    function send(){pend=null;lastSent=Date.now();if(conn)ch.track({p:mine,by:UID}).catch(()=>{})}
    ch.on("presence",{event:"sync"},rebuild).on("presence",{event:"join"},rebuild).on("presence",{event:"leave"},rebuild)
      .subscribe((st,e)=>{const c=st==="SUBSCRIBED";if(c){NET.live="ok"}else if(st==="CHANNEL_ERROR"||st==="TIMED_OUT"){NET.live=st.toLowerCase();note("live "+name,e||st)}
        if(c!==conn){conn=c;cs.forEach(h=>h(c))}if(c)send()});
    const R={name,
      presence:async patch=>{for(const k in patch){if(patch[k]===null)delete mine[k];else mine[k]=patch[k]}rebuild();
        // at most one presence update per 600 ms, always sending the latest state
        if(!pend){const wait=Math.max(0,600-(Date.now()-lastSent));pend=setTimeout(send,wait)}},
      peers:()=>peers,
      onPeers:h=>{hs.add(h);setTimeout(rebuild,0);return()=>hs.delete(h)},
      emit:async()=>{},on:()=>()=>{},
      connected:()=>conn,onConnection:h=>{cs.add(h);setTimeout(()=>h(conn),0);return()=>cs.delete(h)},
      leave:async()=>{hs.clear();try{await sb.removeChannel(ch)}catch(e){}},
      join:async n=>mkRoom("n-"+n)};
    return R}
  let lobby=null;
  window.claude={use:async n=>{UID=await ready;if(!UID)return null;
    if(n==="db")return DB;if(n==="user")return USER;if(n==="room")return lobby||(lobby=mkRoom("lobby"));return null}};
  // one-tap connection check shown in Options
  window.TAMPICO_CHECK=async()=>{const out={};out.auth=NET.auth;
    try{const {count,error}=await sb.from("docs").select("path",{count:"exact",head:true}).eq("col","board");if(error)throw error;out.db="ok ("+count+" leaderboard entries)"}catch(e){out.db="failed: "+(e.message||e.code||e);note("check db",e)}
    out.live=await new Promise(res=>{const ch=sb.channel("tampico-check-"+Math.random().toString(36).slice(2,8));const t=setTimeout(()=>{res("timed out");sb.removeChannel(ch)},8000);
      ch.subscribe((st,e)=>{if(st==="SUBSCRIBED"){clearTimeout(t);res("ok");sb.removeChannel(ch)}else if(st==="CHANNEL_ERROR"||st==="TIMED_OUT"){clearTimeout(t);res(st+(e?": "+(e.message||e):""));sb.removeChannel(ch)}})});
    out.recent=NET.errors.slice(0,5);return out};
  window.TAMPICO_OWNER={get:()=>ownerKey,set:k=>{try{if(k)localStorage.setItem(KEYSTORE,k);else localStorage.removeItem(KEYSTORE)}catch(e){}location.reload()}};
})();
