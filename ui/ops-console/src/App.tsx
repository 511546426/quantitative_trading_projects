import { App as AntdApp } from "antd";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AppShell from "./layout/AppShell";
import DashboardPage from "./pages/DashboardPage";
import InfraPage from "./pages/InfraPage";
import JobsPage from "./pages/JobsPage";
import LogsPage from "./pages/LogsPage";
import StockResearchPage from "./pages/StockResearchPage";

export default function App() {
  return (
    <AntdApp>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<AppShell />}>
            <Route index element={<DashboardPage />} />
            <Route path="infra" element={<InfraPage />} />
            <Route path="jobs" element={<JobsPage />} />
            <Route path="logs" element={<LogsPage />} />
            <Route path="research" element={<StockResearchPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AntdApp>
  );
}
