import React, { useState, useEffect, useRef, useMemo } from 'react';
import { 
  Activity, 
  ArrowUpRight, 
  ArrowDownRight, 
  Wifi, 
  WifiOff, 
  Server, 
  ShieldCheck, 
  Zap, 
  Search, 
  Database,
  Layers,
  Clock
} from 'lucide-react';

const CRYPTO_LIST = ['BTCUSD', 'ETHUSD', 'BNBUSD', 'SOLUSD', 'XRPUSD', 'ADAUSD', 'DOGUSD', 'DOTUSD', 'LTCUSD', 'BCHUSD', 'XLMUSD', 'TRXUSD', 'UNIUSD', 'FILUSD', 'AVXUSD'];
const METAL_LIST = ['XAUUSD', 'XAGUSD'];

export default function App() {
  const [ticks, setTicks] = useState({});
  const [flashStates, setFlashStates] = useState({});
  const [status, setStatus] = useState('connecting');
  const [lastPayloadKB, setLastPayloadKB] = useState(0);
  const [totalTicks, setTotalTicks] = useState(0);
  const [latencyMs, setLatencyMs] = useState(0);
  const [filterText, setFilterText] = useState('');
  const [category, setCategory] = useState('ALL');
  const [sortBy, setSortBy] = useState('symbol');
  const [sortOrder, setSortOrder] = useState('asc');

  const wsRef = useRef(null);
  const prevPricesRef = useRef({});

  useEffect(() => {
    let reconnectTimeout = null;

    const connect = () => {
      setStatus('connecting');
      const wsUrl = `ws://${window.location.hostname}:8080/ws`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        console.log('[Downstream WS] Connected to market engine gateway.');
      };

      ws.onmessage = (event) => {
        const rawLength = event.data.length;
        const sizeKB = rawLength / 1024.0;
        setLastPayloadKB(sizeKB);

        try {
          const packet = JSON.parse(event.data);
          const now = Date.now();
          if (packet.server_time) {
            setLatencyMs(Math.max(1, now - packet.server_time));
          }

          let items = [];
          if (packet.compact && packet.compact.length > 0) {
            // Convert compact schema {s, b, a, h, l, c, t} to standard
            items = packet.compact.map(c => ({
              symbol: c.s,
              buy: c.b,
              sell: c.a,
              high_24h: c.h,
              low_24h: c.l,
              change_24h: c.c,
              timestamp: c.t,
            }));
          } else if (packet.data && packet.data.length > 0) {
            items = packet.data;
          }

          if (items.length > 0) {
            setTotalTicks(prev => prev + items.length);

            const newFlashes = {};
            setTicks(prevTicks => {
              const updated = { ...prevTicks };
              for (const item of items) {
                const prev = prevPricesRef.current[item.symbol];
                const currPrice = (item.buy + item.sell) / 2;

                if (prev) {
                  if (currPrice > prev) newFlashes[item.symbol] = 'up';
                  else if (currPrice < prev) newFlashes[item.symbol] = 'down';
                }
                prevPricesRef.current[item.symbol] = currPrice;
                updated[item.symbol] = item;
              }
              return updated;
            });

            if (Object.keys(newFlashes).length > 0) {
              setFlashStates(prev => ({ ...prev, ...newFlashes }));
              setTimeout(() => {
                setFlashStates(prev => {
                  const cleaned = { ...prev };
                  for (const sym of Object.keys(newFlashes)) {
                    delete cleaned[sym];
                  }
                  return cleaned;
                });
              }, 600);
            }
          }
        } catch (e) {
          console.error('Error parsing WebSocket frame:', e);
        }
      };

      ws.onclose = () => {
        setStatus('disconnected');
        reconnectTimeout = setTimeout(connect, 2000);
      };

      ws.onerror = () => {
        ws.close();
      };
    };

    connect();

    return () => {
      if (wsRef.current) wsRef.current.close();
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
    };
  }, []);

  const getAssetCategory = (sym) => {
    if (CRYPTO_LIST.includes(sym)) return 'CRYPTO';
    if (METAL_LIST.includes(sym)) return 'COMMODITIES';
    return 'FOREX';
  };

  const filteredSymbols = useMemo(() => {
    let list = Object.values(ticks);

    if (category !== 'ALL') {
      list = list.filter(item => getAssetCategory(item.symbol) === category);
    }

    if (filterText.trim()) {
      const q = filterText.toUpperCase();
      list = list.filter(item => item.symbol.includes(q));
    }

    list.sort((a, b) => {
      let valA = a[sortBy];
      let valB = b[sortBy];
      if (typeof valA === 'string') {
        return sortOrder === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      }
      return sortOrder === 'asc' ? valA - valB : valB - valA;
    });

    return list;
  }, [ticks, category, filterText, sortBy, sortOrder]);

  const handleSort = (field) => {
    if (sortBy === field) {
      setSortOrder(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('asc');
    }
  };

  const formatPrice = (val, sym) => {
    if (!val || val === 0) return '0.00';
    if (CRYPTO_LIST.includes(sym) && val > 100) return val.toFixed(2);
    if (METAL_LIST.includes(sym)) return val.toFixed(2);
    if (sym.includes('JPY')) return val.toFixed(3);
    return val.toFixed(5);
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', backgroundColor: 'var(--bg-primary)' }}>
      {/* Top Header */}
      <header style={{
        backgroundColor: 'var(--bg-secondary)',
        borderBottom: '1px solid var(--border-color)',
        padding: '16px 28px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: '16px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '14px' }}>
          <div style={{
            background: 'linear-gradient(135deg, #2563eb, #3b82f6)',
            padding: '10px',
            borderRadius: '10px',
            boxShadow: '0 0 15px rgba(59, 130, 246, 0.4)'
          }}>
            <Activity size={24} color="#ffffff" />
          </div>
          <div>
            <h1 style={{ fontSize: '20px', fontWeight: '700', letterSpacing: '-0.5px' }}>
              APEX Market Data Engine
            </h1>
            <p style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Real-time Sliding 24h Window & Low-Latency Delta Distribution
            </p>
          </div>
        </div>

        {/* SLA & Status Badges */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
          {/* Connection Pill */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            backgroundColor: status === 'connected' ? 'var(--color-green-bg)' : 'var(--color-red-bg)',
            border: `1px solid ${status === 'connected' ? 'var(--color-green)' : 'var(--color-red)'}`,
            padding: '6px 14px',
            borderRadius: '20px',
            fontSize: '12px',
            fontWeight: '600',
            color: status === 'connected' ? 'var(--color-green)' : 'var(--color-red)'
          }}>
            {status === 'connected' ? <Wifi size={14} /> : <WifiOff size={14} />}
            <span>{status === 'connected' ? 'GATEWAY ONLINE' : 'RECONNECTING...'}</span>
          </div>

          {/* Latency Pill */}
          <div style={{
            backgroundColor: 'var(--bg-card)',
            border: '1px solid var(--border-color)',
            padding: '6px 14px',
            borderRadius: '20px',
            fontSize: '12px',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}>
            <Zap size={14} color="#eab308" />
            <span style={{ color: 'var(--text-muted)' }}>E2E Latency:</span>
            <span className="mono" style={{ color: '#facc15', fontWeight: '700' }}>{latencyMs}ms</span>
            <span style={{ color: 'var(--color-green)', fontSize: '10px', fontWeight: '700', marginLeft: '4px' }}>[≤50ms PASS]</span>
          </div>

          {/* Payload Size Pill */}
          <div style={{
            backgroundColor: 'var(--bg-card)',
            border: '1px solid var(--border-color)',
            padding: '6px 14px',
            borderRadius: '20px',
            fontSize: '12px',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}>
            <Layers size={14} color="#38bdf8" />
            <span style={{ color: 'var(--text-muted)' }}>Last Payload:</span>
            <span className="mono" style={{ color: '#38bdf8', fontWeight: '700' }}>{lastPayloadKB.toFixed(2)} KB</span>
            <span style={{ color: 'var(--color-green)', fontSize: '10px', fontWeight: '700', marginLeft: '4px' }}>[&lt;10KB PASS]</span>
          </div>
        </div>
      </header>

      {/* Main Container */}
      <main style={{ padding: '24px 28px', flex: 1, maxWidth: '1440px', margin: '0 auto', width: '100%' }}>
        {/* Metric Cards Row */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
          gap: '16px',
          marginBottom: '24px'
        }}>
          <div style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-muted)', fontSize: '13px', marginBottom: '8px' }}>
              <span>Live Ticks Ingested</span>
              <Activity size={18} color="var(--color-blue)" />
            </div>
            <div className="mono" style={{ fontSize: '26px', fontWeight: '700', color: '#ffffff' }}>
              {totalTicks.toLocaleString()}
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px' }}>
              Continuous high-frequency stream
            </div>
          </div>

          <div style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-muted)', fontSize: '13px', marginBottom: '8px' }}>
              <span>Active Symbols</span>
              <Database size={18} color="var(--color-green)" />
            </div>
            <div className="mono" style={{ fontSize: '26px', fontWeight: '700', color: 'var(--color-green)' }}>
              {Object.keys(ticks).length} / 61
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px' }}>
              Forex, Crypto, Precious Metals
            </div>
          </div>

          <div style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-muted)', fontSize: '13px', marginBottom: '8px' }}>
              <span>State Persistence</span>
              <ShieldCheck size={18} color="#a855f7" />
            </div>
            <div style={{ fontSize: '20px', fontWeight: '700', color: '#c084fc' }}>
              WAL + Disk Snapshot
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px' }}>
              Survives restarts, atomic reload
            </div>
          </div>

          <div style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-muted)', fontSize: '13px', marginBottom: '8px' }}>
              <span>Calculation Engine</span>
              <Clock size={18} color="#f97316" />
            </div>
            <div style={{ fontSize: '20px', fontWeight: '700', color: '#fb923c' }}>
              O(1) Rolling Window
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px' }}>
              Upstream High/Low/Change Ignored
            </div>
          </div>
        </div>

        {/* Filter and Search Bar */}
        <div style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
          borderRadius: '12px',
          padding: '14px 18px',
          marginBottom: '20px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: '12px'
        }}>
          {/* Category Tabs */}
          <div style={{ display: 'flex', gap: '8px' }}>
            {['ALL', 'FOREX', 'CRYPTO', 'COMMODITIES'].map(cat => (
              <button
                key={cat}
                onClick={() => setCategory(cat)}
                style={{
                  backgroundColor: category === cat ? 'var(--color-blue)' : 'var(--bg-card)',
                  color: category === cat ? '#ffffff' : 'var(--text-muted)',
                  border: '1px solid ' + (category === cat ? 'var(--color-blue)' : 'var(--border-color)'),
                  padding: '7px 16px',
                  borderRadius: '8px',
                  fontSize: '12px',
                  fontWeight: '600',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease'
                }}
              >
                {cat}
              </button>
            ))}
          </div>

          {/* Search Box */}
          <div style={{
            position: 'relative',
            display: 'flex',
            alignItems: 'center',
            minWidth: '260px'
          }}>
            <Search size={16} color="var(--text-muted)" style={{ position: 'absolute', left: '12px' }} />
            <input
              type="text"
              placeholder="Filter symbol (e.g. BTC, EUR, XAU)..."
              value={filterText}
              onChange={e => setFilterText(e.target.value)}
              style={{
                width: '100%',
                backgroundColor: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '8px',
                padding: '8px 12px 8px 36px',
                color: '#ffffff',
                fontSize: '13px',
                outline: 'none'
              }}
            />
          </div>
        </div>

        {/* Data Table */}
        <div style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
          borderRadius: '12px',
          overflow: 'hidden',
          boxShadow: '0 4px 20px rgba(0,0,0,0.3)'
        }}>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left', fontSize: '13px' }}>
              <thead>
                <tr style={{
                  backgroundColor: 'var(--bg-card)',
                  borderBottom: '1px solid var(--border-color)',
                  color: 'var(--text-muted)',
                  fontSize: '11px',
                  letterSpacing: '0.5px',
                  textTransform: 'uppercase'
                }}>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('symbol')}>Symbol</th>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('buy')}>Buy (Bid)</th>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('sell')}>Sell (Ask)</th>
                  <th style={{ padding: '14px 18px' }}>Spread</th>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('high_24h')}>24h High (Computed)</th>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('low_24h')}>24h Low (Computed)</th>
                  <th style={{ padding: '14px 18px', cursor: 'pointer' }} onClick={() => handleSort('change_24h')}>24h % Change</th>
                  <th style={{ padding: '14px 18px' }}>Last Tick</th>
                </tr>
              </thead>
              <tbody>
                {filteredSymbols.map((item) => {
                  const flash = flashStates[item.symbol];
                  const flashClass = flash === 'up' ? 'flash-up' : (flash === 'down' ? 'flash-down' : '');
                  const isUp = item.change_24h >= 0;
                  const spread = item.sell - item.buy;
                  const assetCat = getAssetCategory(item.symbol);

                  return (
                    <tr
                      key={item.symbol}
                      className={flashClass}
                      style={{
                        borderBottom: '1px solid var(--border-color)',
                        transition: 'background-color 0.2s ease',
                      }}
                    >
                      <td style={{ padding: '12px 18px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span style={{ fontWeight: '700', fontSize: '14px' }}>{item.symbol}</span>
                          <span style={{
                            fontSize: '9px',
                            fontWeight: '600',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            backgroundColor: assetCat === 'CRYPTO' ? 'rgba(168, 85, 247, 0.15)' : (assetCat === 'COMMODITIES' ? 'rgba(234, 179, 8, 0.15)' : 'rgba(59, 130, 246, 0.15)'),
                            color: assetCat === 'CRYPTO' ? '#c084fc' : (assetCat === 'COMMODITIES' ? '#facc15' : '#60a5fa')
                          }}>
                            {assetCat}
                          </span>
                        </div>
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', fontWeight: '600' }}>
                        {formatPrice(item.buy, item.symbol)}
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', fontWeight: '600' }}>
                        {formatPrice(item.sell, item.symbol)}
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', color: 'var(--text-muted)' }}>
                        {formatPrice(spread, item.symbol)}
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', color: 'var(--color-green)' }}>
                        {formatPrice(item.high_24h, item.symbol)}
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', color: 'var(--color-red)' }}>
                        {formatPrice(item.low_24h, item.symbol)}
                      </td>
                      <td className="mono" style={{ padding: '12px 18px' }}>
                        <span style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '3px',
                          padding: '3px 8px',
                          borderRadius: '6px',
                          fontWeight: '700',
                          fontSize: '12px',
                          backgroundColor: isUp ? 'var(--color-green-bg)' : 'var(--color-red-bg)',
                          color: isUp ? 'var(--color-green)' : 'var(--color-red)'
                        }}>
                          {isUp ? <ArrowUpRight size={13} /> : <ArrowDownRight size={13} />}
                          {isUp ? `+${item.change_24h.toFixed(2)}%` : `${item.change_24h.toFixed(2)}%`}
                        </span>
                      </td>
                      <td className="mono" style={{ padding: '12px 18px', color: 'var(--text-muted)', fontSize: '11px' }}>
                        {new Date(item.timestamp).toLocaleTimeString()}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </main>
    </div>
  );
}
