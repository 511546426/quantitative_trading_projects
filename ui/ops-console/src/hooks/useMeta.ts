import { useEffect, useState } from "react";
import { fetchMeta, type MetaResponse } from "../api/client";

export function useMeta(): MetaResponse | null {
  const [meta, setMeta] = useState<MetaResponse | null>(null);
  useEffect(() => {
    fetchMeta().then(setMeta).catch(() => setMeta(null));
  }, []);
  return meta;
}
