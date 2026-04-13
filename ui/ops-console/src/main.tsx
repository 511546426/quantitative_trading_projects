import React from "react";
import ReactDOM from "react-dom/client";
import { ConfigProvider, theme } from "antd";
import zhCN from "antd/locale/zh_CN";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: "#2f6feb",
          colorBgLayout: "#06090e",
          colorBgContainer: "#0c1017",
          colorBorder: "#1e2836",
          borderRadius: 4,
          fontFamily:
            'ui-sans-serif, system-ui, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif',
        },
        components: {
          Layout: { bodyBg: "#06090e", headerBg: "#0c1017", siderBg: "#0a0d12" },
          Menu: { itemBg: "#0a0d12", darkItemBg: "#0a0d12" },
        },
      }}
    >
      <App />
    </ConfigProvider>
  </React.StrictMode>,
);
