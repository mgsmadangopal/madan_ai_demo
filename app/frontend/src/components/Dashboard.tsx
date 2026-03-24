const DASHBOARD_EMBED_URL =
  'https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f11d227fc917f0994dd67e5cf99167?o=984752964297111';

function Dashboard() {
  return (
    <div className="h-full w-full">
      <iframe
        src={DASHBOARD_EMBED_URL}
        title="ReNew Portfolio Dashboard"
        className="w-full h-full border-0"
        allow="fullscreen"
      />
    </div>
  );
}

export default Dashboard;
