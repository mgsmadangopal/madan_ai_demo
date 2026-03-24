import { useState, useRef, useEffect } from 'react';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

const EXAMPLE_QUESTIONS = [
  'What caused the portfolio net generation drop on 2025-11-21, and what was the total financial impact through December 15?',
  'Which plants and inverter models were affected by the XG-440 firmware rollout, and how much did their availability decline?',
  'What are the TRIP-31 and TRIP-44 alarm codes, and how should dispatch teams respond to them?',
  'How much cumulative O&M spend was incurred after the firmware rollout, and which vendors drove the corrective work?',
  'What does the hotfix released on 2025-12-08 address, and did availability recover after it was applied?',
];

function Chat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const sendMessage = async (content: string) => {
    if (!content.trim() || isLoading) return;

    const userMessage: Message = { role: 'user', content: content.trim() };
    const newMessages = [...messages, userMessage];
    setMessages(newMessages);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: newMessages.map((m) => ({ role: m.role, content: m.content })),
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `Request failed (${response.status})`);
      }

      const data = await response.json();
      setMessages([...newMessages, { role: 'assistant', content: data.response }]);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'An unexpected error occurred';
      setMessages([
        ...newMessages,
        { role: 'assistant', content: `I apologize, but I encountered an error: ${errorMsg}. Please try again.` },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  const handleExampleClick = (question: string) => {
    sendMessage(question);
  };

  const formatContent = (content: string) => {
    // Basic markdown-like formatting
    return content
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/`(.*?)`/g, '<code>$1</code>')
      .replace(/\n/g, '<br />');
  };

  return (
    <div className="h-full flex flex-col max-w-5xl mx-auto w-full">
      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto chat-messages p-4 space-y-4">
        {messages.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full">
            {/* Welcome */}
            <div className="text-center mb-8">
              <div className="w-16 h-16 bg-renew-primary/10 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-renew-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"
                  />
                </svg>
              </div>
              <h2 className="text-2xl font-bold text-renew-primary mb-2">AI Portfolio Assistant</h2>
              <p className="text-gray-500 max-w-lg">
                Ask questions about the firmware incident, portfolio performance, alarm codes, O&M spending, and
                recovery status.
              </p>
            </div>

            {/* Example Questions */}
            <div className="w-full max-w-2xl space-y-2">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 text-center">
                Try asking
              </p>
              {EXAMPLE_QUESTIONS.map((q, i) => (
                <button
                  key={i}
                  onClick={() => handleExampleClick(q)}
                  className="w-full text-left px-4 py-3 bg-white rounded-xl border border-gray-200
                             hover:border-renew-accent hover:shadow-md hover:bg-renew-light
                             transition-all duration-200 text-sm text-gray-700 leading-relaxed"
                >
                  <span className="text-renew-accent font-medium mr-2">Q{i + 1}.</span>
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div
              className={`max-w-[80%] rounded-2xl px-4 py-3 ${
                msg.role === 'user'
                  ? 'bg-renew-primary text-white rounded-br-md'
                  : 'bg-white text-gray-800 shadow-sm border border-gray-100 rounded-bl-md'
              }`}
            >
              {msg.role === 'assistant' && (
                <div className="flex items-center gap-1.5 mb-1.5">
                  <div className="w-5 h-5 bg-renew-primary/10 rounded-full flex items-center justify-center">
                    <svg className="w-3 h-3 text-renew-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                    </svg>
                  </div>
                  <span className="text-xs font-semibold text-renew-primary">AI Assistant</span>
                </div>
              )}
              <div
                className={`text-sm leading-relaxed ${msg.role === 'assistant' ? 'assistant-message' : ''}`}
                dangerouslySetInnerHTML={{ __html: formatContent(msg.content) }}
              />
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white text-gray-800 shadow-sm border border-gray-100 rounded-2xl rounded-bl-md px-4 py-3">
              <div className="flex items-center gap-1.5 mb-1.5">
                <div className="w-5 h-5 bg-renew-primary/10 rounded-full flex items-center justify-center">
                  <svg className="w-3 h-3 text-renew-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                </div>
                <span className="text-xs font-semibold text-renew-primary">AI Assistant</span>
              </div>
              <div className="flex items-center gap-1.5 py-1">
                <span className="text-sm text-gray-500">Analyzing</span>
                <span className="typing-dot w-1.5 h-1.5 bg-renew-accent rounded-full inline-block" />
                <span className="typing-dot w-1.5 h-1.5 bg-renew-accent rounded-full inline-block" />
                <span className="typing-dot w-1.5 h-1.5 bg-renew-accent rounded-full inline-block" />
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input Area */}
      <div className="flex-shrink-0 border-t border-gray-200 bg-white p-4">
        <div className="max-w-3xl mx-auto flex gap-3">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about portfolio performance, firmware incidents, alarm codes..."
            rows={1}
            className="flex-1 resize-none rounded-xl border border-gray-300 px-4 py-3 text-sm
                       focus:outline-none focus:ring-2 focus:ring-renew-accent focus:border-transparent
                       placeholder:text-gray-400"
            disabled={isLoading}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={!input.trim() || isLoading}
            className="px-5 py-3 bg-renew-primary text-white rounded-xl font-medium text-sm
                       hover:bg-renew-dark transition-colors duration-200
                       disabled:opacity-40 disabled:cursor-not-allowed
                       flex items-center gap-2"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
            </svg>
            Send
          </button>
        </div>
        <p className="text-center text-xs text-gray-400 mt-2">
          Powered by Databricks AI - Responses may take a moment as the agent analyzes portfolio data
        </p>
      </div>
    </div>
  );
}

export default Chat;
