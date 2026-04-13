import axios, { type AxiosResponse } from "axios";
import { setHttpTrace } from "./traceStore";

const client = axios.create({
  baseURL: "",
  timeout: 125_000,
});

client.interceptors.request.use((config) => {
  const k = localStorage.getItem("quant_ops_api_key");
  if (k) {
    config.headers["X-API-Key"] = k;
  }
  return config;
});

function readHeader(headers: AxiosResponse["headers"], name: string): string | null {
  const v = headers[name] ?? headers[name.toLowerCase()];
  return typeof v === "string" ? v : null;
}

client.interceptors.response.use(
  (res) => {
    setHttpTrace(
      readHeader(res.headers, "x-request-id"),
      readHeader(res.headers, "x-server-time"),
    );
    return res;
  },
  (err: unknown) => {
    if (axios.isAxiosError(err) && err.response) {
      setHttpTrace(
        readHeader(err.response.headers, "x-request-id"),
        readHeader(err.response.headers, "x-server-time"),
      );
    }
    return Promise.reject(err);
  },
);

export default client;

export type MetaResponse = {
  project_dir: string;
  ops_sh: string;
  python: string;
  auth_required: boolean;
  log_paths: Record<string, string>;
  server_time_utc?: string;
  build_id?: string;
};

export async function fetchMeta(): Promise<MetaResponse> {
  const { data } = await client.get<MetaResponse>("/api/meta");
  return data;
}

export type HealthResponse = {
  ok: boolean;
  service: string;
  server_time_utc: string;
  version?: string;
  build_id?: string;
};

export async function fetchHealth(): Promise<HealthResponse> {
  const { data } = await client.get<HealthResponse>("/api/health");
  return data;
}
