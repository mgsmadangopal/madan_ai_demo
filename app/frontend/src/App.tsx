import { useState } from 'react';
import Dashboard from './components/Dashboard';
import Chat from './components/Chat';

type Tab = 'dashboard' | 'assistant';

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('dashboard');

  return (
    <div className="h-full flex flex-col bg-renew-secondary">
      {/* Header */}
      <header className="bg-renew-primary text-white shadow-lg flex-shrink-0">
        <div className="max-w-full mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              {/* Logo / Icon */}
              <div className="w-10 h-10 bg-white/20 rounded-lg flex items-center justify-center">
                <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <div>
                <h1 className="text-xl font-bold tracking-tight">ReNew Capital Partners</h1>
                <p className="text-sm text-green-200 font-medium">AI Portfolio Intelligence</p>
              </div>
            </div>
            <p className="text-sm text-green-200 hidden md:block">
              Firmware Incident Analysis & Portfolio Performance
            </p>
          </div>
        </div>

        {/* Tab Navigation */}
        <div className="px-6">
          <nav className="flex gap-1">
            <button
              onClick={() => setActiveTab('dashboard')}
              className={`px-5 py-2.5 text-sm font-semibold rounded-t-lg transition-all duration-200 ${
                activeTab === 'dashboard'
                  ? 'bg-renew-secondary text-renew-primary'
                  : 'text-green-200 hover:bg-white/10 hover:text-white'
              }`}
            >
              <span className="flex items-center gap-2">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                Dashboard
              </span>
            </button>
            <button
              onClick={() => setActiveTab('assistant')}
              className={`px-5 py-2.5 text-sm font-semibold rounded-t-lg transition-all duration-200 ${
                activeTab === 'assistant'
                  ? 'bg-renew-secondary text-renew-primary'
                  : 'text-green-200 hover:bg-white/10 hover:text-white'
              }`}
            >
              <span className="flex items-center gap-2">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
                </svg>
                AI Assistant
              </span>
            </button>
          </nav>
        </div>
      </header>

      {/* Content Area */}
      <main className="flex-1 overflow-hidden">
        {activeTab === 'dashboard' ? <Dashboard /> : <Chat />}
      </main>
    </div>
  );
}

export default App;
