import fs from "fs";
import path from "path";
import fetch from "node-fetch";
import AbortController from "abort-controller";

const HTTP_TIMEOUT = 15000;
const DEFAULT_TTL = Number(process.env.CACHE_TTL_SECONDS || 600);
const DATAGOV_API_KEY = process.env.DATAGOV_API_KEY || "";
const DATAGOV_DATASET_IDS_GLOBAL = process.env.DATAGOV_DATASET_IDS
  ? JSON.parse(process.env.DATAGOV_DATASET_IDS)
  : [];
const RIVERS_FILE = path.join(process.cwd(), "data", "rivers.json");

const DEFAULT_WQ_CONFIG = {
  weights: { DO: 0.3, BOD: 0.25, pH: 0.15, Coliforms: 0.2, Others: 0.1 },
  thresholds: {
    DO: { excellent: 6, good: 5, moderate: 4, poor: 2 },
    BOD: { excellent: 2, good: 3, moderate: 5, poor: 6 },
    pH: { min: 6.5, max: 8.5 },
    Coliforms: { excellent: 50, good: 500, moderate: 5000 },
  },
  freshness_seconds: 60 * 60 * 24 * 7,
};
const WQ_CONFIG = process.env.WQ_CONFIG_JSON
  ? JSON.parse(process.env.WQ_CONFIG_JSON)
  : DEFAULT_WQ_CONFIG;

const cache = new Map();
function setCache(k, v, ttl = DEFAULT_TTL) {
  cache.set(k, { v, expires: Date.now() + ttl * 1000 });
}
function getCache(k) {
  const z = cache.get(k);
  if (!z) return null;
  if (Date.now() > z.expires) {
    cache.delete(k);
    return null;
  }
  return z.v;
}

async function timeoutFetch(url, opts = {}) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), HTTP_TIMEOUT);
  try {
    const r = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return r;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
}

function tryNum(v) {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).replace(/[^0-9.\-eE]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function loadRivers() {
  try {
    const raw = fs.readFileSync(RIVERS_FILE, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return [];
  }
}

function findRiver(q) {
  if (!q) return null;
  const rs = loadRivers();
  const low = q.toLowerCase().trim();
  let r = rs.find((x) => x.id === low || x.name.toLowerCase() === low);
  if (r) return r;
  r = rs.find((x) => x.aliases && x.aliases.map((a) => a.toLowerCase()).includes(low));
  if (r) return r;
  return (
    rs.find(
      (x) =>
        x.name.toLowerCase().includes(low) ||
        (x.aliases && x.aliases.some((a) => a.toLowerCase().includes(low)))
    ) || null
  );
}

async function fetchRecordsFromDataGovByResource(resourceId) {
  if (!resourceId) return null;
  try {
    const url =
      `https://data.gov.in/api/datastore/resource/search.json?resource_id=${resourceId}` +
      (DATAGOV_API_KEY ? `&api-key=${DATAGOV_API_KEY}` : "") +
      "&limit=500";

    const r = await timeoutFetch(url, { headers: { Accept: "application/json" } });
    if (!r.ok) throw new Error("DataGov error: " + r.status);
    const j = await r.json();
    return j.records || j.data || null;
  } catch (e) {
    return null;
  }
}

async function searchDataGovCatalog(riverName) {
  const q = encodeURIComponent(riverName);
  const urls = [
    `https://data.gov.in/api/datastore/resource/search.json?filters[river_name]=${q}&limit=200`,
    `https://data.gov.in/api/datastore/resource/search.json?filters[station_name]=${q}&limit=200`,
  ];
  for (const u of urls) {
    try {
      const url = DATAGOV_API_KEY ? u + `&api-key=${DATAGOV_API_KEY}` : u;
      const r = await timeoutFetch(url, { headers: { Accept: "application/json" } });
      const j = await r.json();
      if (j.records?.length) return j.records;
    } catch {}
  }
  return null;
}

function mapRecord(row) {
  return {
    DO: tryNum(row.DO || row["Dissolved Oxygen"]),
    BOD: tryNum(row.BOD || row["Biochemical Oxygen Demand"]),
    pH: tryNum(row.pH || row["pH"]),
    Coliforms: tryNum(row.total_coliform || row["Total Coliform"]),
    timestamp: row.date || row.timestamp || null,
  };
}

function pickBest(records) {
  let best = null;
  let score = -1;
  for (const r of records.map(mapRecord)) {
    let s = 0;
    ["DO", "BOD", "pH", "Coliforms"].forEach((k) => {
      if (r[k] !== null && r[k] !== undefined) s++;
    });
    if (s > score) {
      score = s;
      best = r;
    }
  }
  return best;
}

function paramIndex(param, value) {
  if (value == null) return null;
  const t = WQ_CONFIG.thresholds;
  switch (param) {
    case "DO":
      return Math.min(100, (value / t.DO.excellent) * 100);
    case "BOD":
      return Math.max(0, 100 * (1 - value / (t.BOD.moderate * 2)));
    case "pH":
      if (value >= t.pH.min && value <= t.pH.max) return 100;
      return Math.max(0, 100 - Math.abs(value - 7.5) * 20);
    case "Coliforms":
      return Math.max(0, 100 - Math.log10(value + 1) * 20);
  }
}

function computeRWQI(p) {
  const w = WQ_CONFIG.weights;
  const sub = {};
  let n = 0,
    d = 0;

  for (const k of ["DO", "BOD", "pH", "Coliforms"]) {
    const si = paramIndex(k, p[k]);
    if (si != null) {
      sub[k] = Math.round(si * 10) / 10;
      n += si * w[k];
      d += w[k];
    }
  }
  if (d === 0) return { rwqi: null, category: null, subindices: sub };

  const score = Math.round(((n / d) * 10)) / 10;
  let cat = "Bad";
  if (score >= 90) cat = "Excellent";
  else if (score >= 75) cat = "Good";
  else if (score >= 50) cat = "Moderate";
  else if (score >= 25) cat = "Poor";

  return { rwqi: score, category: cat, subindices: sub };
}

export default async function handler(req, res) {
  try {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET");

    const q = (req.query.river || "").toString().trim();
    if (!q) return res.status(400).json({ status: "error", message: "river query required" });

    const river = findRiver(q);
    if (!river) return res.status(404).json({ status: "error", message: "unknown river" });

    const cacheKey = "rwqi::" + river.id;
    const cached = getCache(cacheKey);
    if (cached) return res.json(cached);

    let records = null;

    if (river.dataset_ids?.length) {
      for (const id of river.dataset_ids) {
        records = await fetchRecordsFromDataGovByResource(id);
        if (records?.length) break;
      }
    }

    if (!records && DATAGOV_DATASET_IDS_GLOBAL.length) {
      for (const id of DATAGOV_DATASET_IDS_GLOBAL) {
        records = await fetchRecordsFromDataGovByResource(id);
        if (records?.length) break;
      }
    }

    if (!records) {
      records = await searchDataGovCatalog(river.name);
    }

    if (!records?.length) {
      const out = { river: river.name, status: "coming_soon" };
      setCache(cacheKey, out);
      return res.json(out);
    }

    const params = pickBest(records);
    const result = computeRWQI(params);

    const out = {
      river: river.name,
      rwqi: result.rwqi,
      category: result.category,
      subindices: result.subindices,
      parameters: params,
      status: "ok",
    };

    setCache(cacheKey, out);
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ status: "error", message: e.toString() });
  }
}
