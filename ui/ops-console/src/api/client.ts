import axios from "axios";

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

export default client;

export type MetaResponse = {
  project_dir: string;
  ops_sh: string;
  python: string;
  auth_required: boolean;
  log_paths: Record<string, string>;
};

export async function fetchMeta(): Promise<MetaResponse> {
  const { data } = await client.get<MetaResponse>("/api/meta");
  return data;
}
