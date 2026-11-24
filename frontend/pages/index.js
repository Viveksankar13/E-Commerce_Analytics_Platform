import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  TrendingUp, 
  ShoppingBag, 
  Eye, 
  RefreshCw, 
  AlertCircle, 
  Zap, 
  Activity, 
  CheckCircle,
  Clock,
  ArrowUpRight
} from 'lucide-react';

const API_URL = 'http://localhost:8000/api/v1/products/trending';

export default function Dashboard() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState('connecting'); // 'connecting' | 'online' | 'offline'
  const [lastRefreshed, setLastRefreshed] = useState(null);
  const [secondsToRefresh, setSecondsToRefresh] = useState(5);

  const fetchTrendingData = async () => {
    try {
      setConnectionStatus('connecting');
      const response = await axios.get(API_URL);
      setProducts(response.data);
      setLastRefreshed(new Date().toLocaleTimeString());
      setConnectionStatus('online');
      setError(null);
    } catch (err) {
      console.error("Error fetching trending products:", err);
      setConnectionStatus('offline');
      setError("Unable to connect to the real-time API. Displaying local simulated data.");
      
      // If we don't have products yet, generate mock product listings for testing/dev visual validation
      if (products.length === 0) {
        const mockProducts = Array.from({ length: 10 }, (_, i) => {
          const rank = i + 1;
          const clicks = 350 - rank * 25 + Math.floor(Math.random() * 15);
          const orders = 45 - rank * 4 + Math.floor(Math.random() * 3);
          const score = clicks * 1.0 + orders * 5.0;
          return {
            product_id: `PROD_${String(Math.floor(Math.random() * 900) + 100)}`,
            click_count: clicks,
            order_count: orders,
            popularity_index: score,
            last_updated: new Date().toISOString()
          };
        }).sort((a, b) => b.popularity_index - a.popularity_index);
        setProducts(mockProducts);
      }
    } finally {
      setLoading(false);
    }
  };

  // Initial and polling fetch
  useEffect(() => {
    fetchTrendingData();
    
    // Poll data every 5 seconds
    const dataInterval = setInterval(() => {
      fetchTrendingData();
      setSecondsToRefresh(5);
    }, 5000);

    // Countdown timer for next refresh
    const countdownInterval = setInterval(() => {
      setSecondsToRefresh((prev) => (prev > 1 ? prev - 1 : 5));
    }, 1000);

    return () => {
      clearInterval(dataInterval);
      clearInterval(countdownInterval);
    };
  }, []);

  // Compute aggregated stats
  const totalClicks = products.reduce((acc, curr) => acc + (curr.click_count || 0), 0);
  const totalOrders = products.reduce((acc, curr) => acc + (curr.order_count || 0), 0);
  const conversionRate = totalClicks > 0 ? ((totalOrders / totalClicks) * 100).toFixed(1) : 0;
  const maxPopularity = products.length > 0 ? Math.max(...products.map(p => p.popularity_index || 1)) : 1;

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 selection:bg-indigo-500 selection:text-white pb-12">
      {/* Background ambient glows */}
      <div className="absolute top-0 left-1/4 w-96 h-96 bg-indigo-900/10 rounded-full blur-[120px] pointer-events-none" />
      <div className="absolute top-1/3 right-1/4 w-96 h-96 bg-emerald-950/10 rounded-full blur-[120px] pointer-events-none" />

      {/* Header Bar */}
      <header className="border-b border-zinc-900 bg-zinc-950/80 backdrop-blur-md sticky top-0 z-50 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <div className="flex items-center gap-2">
              <span className="p-1.5 rounded-lg bg-indigo-600 text-white animate-pulse-slow">
                <Zap className="h-5 w-5" />
              </span>
              <h1 className="text-xl font-bold tracking-tight text-white">E-commerce Analytics</h1>
            </div>
            <p className="text-xs text-zinc-400 mt-1">Real-Time Inventory & Dynamic Pricing Orchestrator</p>
          </div>
          
          {/* Status & Sync metrics */}
          <div className="flex flex-wrap items-center gap-4 text-sm">
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-zinc-900 border border-zinc-800">
              <span className={`h-2.5 w-2.5 rounded-full ${
                connectionStatus === 'online' ? 'bg-emerald-500 animate-pulse' :
                connectionStatus === 'connecting' ? 'bg-amber-500 animate-spin' : 'bg-rose-500'
              }`} />
              <span className="text-xs font-medium capitalize text-zinc-300">
                {connectionStatus === 'online' ? 'System Live' : 
                 connectionStatus === 'connecting' ? 'Syncing...' : 'Disconnected (Demo Mode)'}
              </span>
            </div>

            <div className="flex items-center gap-1.5 text-zinc-400 text-xs">
              <Clock className="h-3.5 w-3.5" />
              <span>Next refresh: {secondsToRefresh}s</span>
            </div>

            <button 
              onClick={fetchTrendingData}
              className="p-1.5 rounded-lg bg-zinc-900 border border-zinc-800 hover:bg-zinc-800 hover:border-zinc-700 transition text-zinc-300"
              title="Force Refresh"
            >
              <RefreshCw className={`h-4 w-4 ${connectionStatus === 'connecting' ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 mt-8">
        {/* Error/Warning Alert */}
        {error && (
          <div className="mb-6 p-4 rounded-xl bg-amber-950/20 border border-amber-900/40 text-amber-300 flex items-start gap-3 text-sm">
            <AlertCircle className="h-5 w-5 shrink-0 mt-0.5" />
            <div>
              <span className="font-semibold">Local Environment Warning: </span>
              {error}
            </div>
          </div>
        )}

        {/* Aggregate KPI Panels */}
        <section className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-zinc-900/30 border border-zinc-900 rounded-2xl p-6 backdrop-blur-sm relative overflow-hidden group hover:border-indigo-900/50 transition duration-300">
            <div className="absolute top-0 right-0 w-24 h-24 bg-indigo-500/5 rounded-bl-full pointer-events-none transition group-hover:bg-indigo-500/10" />
            <div className="flex justify-between items-center">
              <span className="text-zinc-400 text-xs font-semibold uppercase tracking-wider">Top-10 Clicks (10m)</span>
              <span className="p-2 rounded-xl bg-zinc-950 text-zinc-400"><Eye className="h-4 w-4" /></span>
            </div>
            <div className="mt-4 flex items-baseline gap-2">
              <span className="text-3xl font-bold text-white">{loading ? '...' : totalClicks.toLocaleString()}</span>
              <span className="text-xs text-indigo-400 font-medium">Views Cached</span>
            </div>
          </div>

          <div className="bg-zinc-900/30 border border-zinc-900 rounded-2xl p-6 backdrop-blur-sm relative overflow-hidden group hover:border-emerald-900/50 transition duration-300">
            <div className="absolute top-0 right-0 w-24 h-24 bg-emerald-500/5 rounded-bl-full pointer-events-none transition group-hover:bg-emerald-500/10" />
            <div className="flex justify-between items-center">
              <span className="text-zinc-400 text-xs font-semibold uppercase tracking-wider">Top-10 Orders (10m)</span>
              <span className="p-2 rounded-xl bg-zinc-950 text-zinc-400"><ShoppingBag className="h-4 w-4" /></span>
            </div>
            <div className="mt-4 flex items-baseline gap-2">
              <span className="text-3xl font-bold text-white">{loading ? '...' : totalOrders.toLocaleString()}</span>
              <span className="text-xs text-emerald-400 font-medium">Purchases</span>
            </div>
          </div>

          <div className="bg-zinc-900/30 border border-zinc-900 rounded-2xl p-6 backdrop-blur-sm relative overflow-hidden group hover:border-violet-900/50 transition duration-300">
            <div className="absolute top-0 right-0 w-24 h-24 bg-violet-500/5 rounded-bl-full pointer-events-none transition group-hover:bg-violet-500/10" />
            <div className="flex justify-between items-center">
              <span className="text-zinc-400 text-xs font-semibold uppercase tracking-wider">Conversion Ratio</span>
              <span className="p-2 rounded-xl bg-zinc-950 text-zinc-400"><Activity className="h-4 w-4" /></span>
            </div>
            <div className="mt-4 flex items-baseline gap-2">
              <span className="text-3xl font-bold text-white">{loading ? '...' : `${conversionRate}%`}</span>
              <span className="text-xs text-violet-400 font-medium">Orders/Clicks</span>
            </div>
          </div>
        </section>

        {/* Section Title */}
        <div className="flex justify-between items-center mb-6">
          <div>
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              <TrendingUp className="h-5 w-5 text-indigo-500" />
              Trending Now
            </h2>
            <p className="text-xs text-zinc-400 mt-1">Based on formula: (Clicks * 1.0) + (Orders * 5.0) in sliding 10m window</p>
          </div>
          {lastRefreshed && (
            <span className="text-xs text-zinc-500">Last updated: {lastRefreshed}</span>
          )}
        </div>

        {/* Trending Product Grid */}
        {loading && products.length === 0 ? (
          <div className="py-24 text-center">
            <RefreshCw className="h-8 w-8 animate-spin mx-auto text-indigo-500" />
            <p className="text-sm text-zinc-400 mt-4">Connecting to ingestion server...</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {products.map((item, index) => {
              const rank = index + 1;
              const fillPct = Math.min(((item.popularity_index || 0) / maxPopularity) * 100, 100);
              
              return (
                <div 
                  key={item.product_id}
                  className="bg-zinc-900/20 border border-zinc-900/60 rounded-2xl p-6 backdrop-blur-sm transition duration-300 hover:scale-[1.01] hover:border-zinc-800 hover:shadow-xl hover:shadow-indigo-500/5 group"
                >
                  {/* Top line of card */}
                  <div className="flex justify-between items-center mb-4">
                    <span className={`text-xs font-extrabold px-2.5 py-1 rounded-full ${
                      rank === 1 ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20' :
                      rank === 2 ? 'bg-slate-300/10 text-slate-300 border border-slate-300/20' :
                      rank === 3 ? 'bg-amber-700/10 text-amber-600 border border-amber-700/20' :
                      'bg-zinc-900 text-zinc-400 border border-zinc-800'
                    }`}>
                      #{rank}
                    </span>
                    
                    {/* Tiny link to details mock */}
                    <span className="text-zinc-600 group-hover:text-indigo-400 transition">
                      <ArrowUpRight className="h-4 w-4" />
                    </span>
                  </div>

                  {/* Product ID */}
                  <h3 className="text-2xl font-black text-white tracking-tight">{item.product_id}</h3>
                  <p className="text-[10px] text-zinc-500 mt-0.5">Updated: {new Date(item.last_updated).toLocaleTimeString()}</p>

                  {/* Popularity score meter */}
                  <div className="mt-5">
                    <div className="flex justify-between items-baseline mb-1">
                      <span className="text-xs text-zinc-400">Popularity Score</span>
                      <span className="text-sm font-bold text-indigo-400">{Math.round(item.popularity_index).toLocaleString()}</span>
                    </div>
                    {/* Score Bar */}
                    <div className="h-1.5 w-full bg-zinc-900 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full transition-all duration-1000"
                        style={{ width: `${fillPct}%` }}
                      />
                    </div>
                  </div>

                  {/* Click / Order Split */}
                  <div className="grid grid-cols-2 gap-4 mt-6 pt-4 border-t border-zinc-900">
                    <div className="flex items-center gap-2">
                      <span className="p-1.5 rounded-lg bg-zinc-900 text-zinc-400"><Eye className="h-3.5 w-3.5" /></span>
                      <div>
                        <div className="text-[10px] text-zinc-500 leading-none">Views</div>
                        <div className="text-sm font-bold text-zinc-300 mt-1">{item.click_count || 0}</div>
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-2">
                      <span className="p-1.5 rounded-lg bg-zinc-900 text-zinc-400"><ShoppingBag className="h-3.5 w-3.5" /></span>
                      <div>
                        <div className="text-[10px] text-zinc-500 leading-none">Orders</div>
                        <div className="text-sm font-bold text-zinc-300 mt-1">{item.order_count || 0}</div>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </main>

      {/* Footer Info */}
      <footer className="max-w-7xl mx-auto px-6 mt-16 pt-6 border-t border-zinc-900 text-center text-xs text-zinc-600 flex flex-col md:flex-row justify-between items-center gap-4">
        <div>
          Platform Engine: PySpark Structured Streaming & Kafka & Cassandra & Airflow
        </div>
        <div className="flex items-center gap-1.5">
          <CheckCircle className="h-4 w-4 text-emerald-600" />
          <span>Local Cassandra Keyspace Ready</span>
        </div>
      </footer>
    </div>
  );
}
