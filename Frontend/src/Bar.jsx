import React, { useState } from 'react';

const Bar = () => {
  const [command, setCommand] = useState('SET');
    const [key, setKey] = useState('');
    const [val, setVal] = useState('');
    const [response, setResponse] = useState('');
  
    const handleExecute = async (e) => {
      e.preventDefault();
      if (!key.trim()) {
        setResponse('(error) ERR wrong number of arguments (key missing)');
        return;
      }
      const res = await fetch("http://localhost:7002",{
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ Cmd: command,Key: key, Val: val }),
      })
      const data = await res.json();
      setResponse(data.Response);
    };
  
    const handleCommandChange = (e) => {
      const newCommand = e.target.value;
      setCommand(newCommand);
      if (newCommand !== 'SET') setVal('');
      setResponse('');
    };
  
    const isValueDisabled = command === 'GET' || command === 'DEL';
  
    return (
      <div className="max-w-5xl mx-auto p-8 bg-white text-black font-mono min-h-screen selection:bg-gray-200">
        
        {/* Top Menu Elements */}
        {/* <div className="flex justify-between items-center text-sm mb-16">
          <span className="underline cursor-pointer decoration-black underline-offset-4">play</span>
          <div className="flex items-center gap-2">
            <span className="cursor-pointer">[copy link]</span>
            <span className="text-xs">▲</span>
          </div>
        </div>*/}
  
        {/* Header */}
        <h1 className="text-4xl mb-6">KV store</h1>
  
        {/* Main Form */}
        <form onSubmit={handleExecute}>
          
          {/* Command and Key Row */}
          <div className="flex flex-col md:flex-row gap-6 mb-8">
            <div className="w-full md:w-1/4">
              <label className="block mb-2">Command</label>
              <div className="relative">
                <select
                  value={command}
                  onChange={handleCommandChange}
                  className="w-full border border-gray-300 p-3 bg-white text-black appearance-none rounded-none focus:outline-none focus:border-black"
                >
                  <option value="SET">SET</option>
                  <option value="GET">GET</option>
                  <option value="DEL">DEL</option>
                </select>
                <div className="absolute inset-y-0 right-3 flex items-center pointer-events-none text-gray-400">
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </div>
              </div>
            </div>
            
            <div className="w-full md:w-3/4">
              <label className="block mb-2">Key</label>
              <input
                type="text"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="Key"
                className="w-full border border-gray-300 p-3 bg-white text-black rounded-none focus:outline-none focus:border-black"
              />
            </div>
          </div>
  
          {/* Value Row */}
          <div className="mb-8">
            <label className="block mb-2">Value</label>
            <input
              type="text"
              value={val}
              onChange={(e) => setVal(e.target.value)}
              disabled={isValueDisabled}
              placeholder={isValueDisabled ? "" : "enter value here..."}
              className={`w-full border border-gray-300 p-3 rounded-none focus:outline-none focus:border-black text-black ${
                isValueDisabled ? 'bg-gray-50 opacity-60 cursor-not-allowed' : 'bg-white'
              }`}
            />
          </div>
  
          {/* Execute Button */}
          <button
            type="submit"
            className="rounded-xs cursor-pointer w-full px-8 py-3 border border-gray-300 bg-white text-black hover:bg-gray-50 text-center  transition-colors"
          >
            Execute Command
          </button>
        </form>
  
        {/* Output / Response Row */}
        {response && (
          <div className="mt-12">
            <label className="block mb-2">Response Output</label>
            <div className="w-full border border-gray-300 p-4 min-h-[120px] bg-white text-black whitespace-pre-wrap rounded-none">
              {response}
            </div>
          </div>
        )}
        
      </div>
    );
};

export default Bar;