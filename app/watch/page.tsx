'use client';

import { useEffect, useState } from 'react';
import * as mediasoupClient from 'mediasoup-client';
import io, { Socket } from 'socket.io-client';

interface Producer {
  id: string;
  kind: string;
  userId?: string;
  userName?: string;
}

interface StreamVideo {
  producerId: string;
  kind: string;
  userId: string;
  userName?: string;
  stream: MediaStream;
}

export default function Watch() {
  const [socket, setSocket] = useState<Socket | null>(null);
  const [status, setStatus] = useState('Connecting...');
  const [availableProducers, setAvailableProducers] = useState<Producer[]>([]);
  const [activeStreams, setActiveStreams] = useState<StreamVideo[]>([]);
  const [device, setDevice] = useState<mediasoupClient.Device | null>(null);
  const [recvTransport, setRecvTransport] = useState<mediasoupClient.types.Transport | null>(null);
  const [mySocketId, setMySocketId] = useState<string | null>(null);
  const [autoConsumeEnabled, setAutoConsumeEnabled] = useState(true);

  useEffect(() => {
    const initializeWatcher = async () => {
      try {
        setStatus('Connecting to server...');
        const socketConnection = io('https://webrtc-live.onrender.com');
        setSocket(socketConnection);

        socketConnection.on('connect', () => {
          setStatus('Connected to server');
          setMySocketId(socketConnection.id || null);
          socketConnection.emit('getAvailableProducers', (producers: Producer[]) => {
            setAvailableProducers(producers);
          });
        });

        socketConnection.on('disconnect', () => {
          setStatus('Disconnected from server');
          setActiveStreams([]);
        });
        socketConnection.on('newProducer', (data: { producerId: string; kind: string; userId: string; userName?: string }) => {
          const newProducer = { 
            id: data.producerId, 
            kind: data.kind, 
            userId: data.userId,
            userName: data.userName
          };
          setAvailableProducers(prev => [
            ...prev.filter(p => p.id !== data.producerId),
            newProducer
          ]);
        });

        socketConnection.on('producerClosed', (data: { producerId: string }) => {
          setAvailableProducers(prev => prev.filter(p => p.id !== data.producerId));
          setActiveStreams(prev => prev.filter(stream => stream.producerId !== data.producerId));
        });

        socketConnection.on('activeProducers', (producers: Producer[]) => {
          setAvailableProducers(producers.map(p => ({
            ...p,
            userName: p.userName
          })));
        });

        const mediasoupDevice = new mediasoupClient.Device();
        const rtpCapabilities = await new Promise<mediasoupClient.types.RtpCapabilities>((resolve) => {
          socketConnection.emit('getRtpCapabilities', resolve);
        });
        await mediasoupDevice.load({ routerRtpCapabilities: rtpCapabilities });
        setDevice(mediasoupDevice);
        const transportOptions = await new Promise<mediasoupClient.types.TransportOptions>((resolve) => {
          socketConnection.emit('createTransport', resolve);
        });
        const transport = mediasoupDevice.createRecvTransport(transportOptions);
        transport.on('connect', ({ dtlsParameters }, callback) => {
          socketConnection.emit('connectTransport', { 
            transportId: transportOptions.id, 
            dtlsParameters 
          }, (response: { error?: string }) => {
            if (response.error) {
              callback();
            } else {
              callback();
            }
          });
        });
        setRecvTransport(transport);
        setStatus('Ready to watch streams');
      } catch (error) {
        setStatus(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    };
    initializeWatcher();
    return () => {
      if (socket) {
        socket.disconnect();
      }
    };
  }, []);

  useEffect(() => {
    if (autoConsumeEnabled && socket && device && recvTransport && availableProducers.length > 0) {
      availableProducers.forEach(producer => {
        const alreadyConsumed = activeStreams.some(stream => stream.producerId === producer.id);
        if (!alreadyConsumed) {
          setTimeout(() => {
            consumeStream(producer);
          }, 500);
        }
      });
    }
  }, [autoConsumeEnabled, socket, device, recvTransport, availableProducers]);

  const consumeStream = async (producer: Producer) => {
    if (!socket || !device || !recvTransport) {
      return;
    }

    try {
      const consumerParams = await new Promise<mediasoupClient.types.ConsumerOptions & { error?: string }>((resolve) => {
        socket.emit('consume', {
          transportId: recvTransport.id,
          producerId: producer.id,
          rtpCapabilities: device.rtpCapabilities,
        }, resolve);
      });
      if (consumerParams.error) {
        throw new Error(consumerParams.error);
      }
      const consumer = await recvTransport.consume({
        id: consumerParams.id,
        producerId: producer.id,
        kind: consumerParams.kind,
        rtpParameters: consumerParams.rtpParameters,
      });
      setActiveStreams(prev => {
        const existingStreamIndex = prev.findIndex(s => s.userId === producer.userId);
        if (existingStreamIndex !== -1) {
          const updatedStreams = [...prev];
          updatedStreams[existingStreamIndex].stream.addTrack(consumer.track);
          updatedStreams[existingStreamIndex] = {
            ...updatedStreams[existingStreamIndex],
            kind: updatedStreams[existingStreamIndex].stream.getTracks().map(t => t.kind).join('+'),
            userName: producer.userName || updatedStreams[existingStreamIndex].userName
          };
          return updatedStreams;
        } else {
          const newStream = new MediaStream();
          newStream.addTrack(consumer.track);
          const streamVideo: StreamVideo = {
            producerId: producer.id,
            kind: consumer.track.kind,
            userId: producer.userId || 'unknown',
            userName: producer.userName,
            stream: newStream,
          };
          return [...prev, streamVideo];
        }
      });
      setAvailableProducers(prev => prev.filter(p => p.id !== producer.id));
    } catch (error) {
      setStatus(`Error consuming stream: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  return (
    <div style={{ padding: '20px', maxWidth: '1200px', margin: '0 auto' }}>
      <h1>Auto-Watch Live Streams</h1>
      
      <div style={{ marginBottom: '20px' }}>
        <strong>Status:</strong> {status}
      </div>

      <div style={{ marginBottom: '20px', fontSize: '14px', color: '#666' }}>
        <strong>Auto-Streaming Info:</strong><br />
        Connected: {socket ? 'Yes' : 'No'}<br />
        Active Streams: {activeStreams.length}<br />
        Auto-consumption: {autoConsumeEnabled ? 'Enabled' : 'Disabled'}
        <br />
        <div style={{ marginTop: '10px', display: 'flex', gap: '10px' }}>
          <button
            onClick={() => {
              if (socket) {
                socket.emit('getAvailableProducers', (producers: Producer[]) => {
                  setAvailableProducers(producers);
                  producers.forEach(producer => {
                    const alreadyConsumed = activeStreams.some(stream => stream.producerId === producer.id);
                    if (!alreadyConsumed) {
                      setTimeout(() => consumeStream(producer), 100);
                    }
                  });
                });
              }
            }}
            style={{
              padding: '5px 10px',
              fontSize: '12px',
              backgroundColor: '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer',
            }}
          >
            Refresh Streams
          </button>
          
          <button
            onClick={() => setAutoConsumeEnabled(!autoConsumeEnabled)}
            style={{
              padding: '5px 10px',
              fontSize: '12px',
              backgroundColor: autoConsumeEnabled ? '#28a745' : '#dc3545',
              color: 'white',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer',
            }}
          >
            {autoConsumeEnabled ? 'Disable' : 'Enable'} Auto-Watch
          </button>
        </div>
      </div>
      {activeStreams.length > 0 && (
        <div style={{ marginBottom: '30px' }}>
          <h2>Live Streams ({activeStreams.length} active)</h2>
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
            gap: '20px',
            marginBottom: '20px'
          }}>
            {Array.from(
              activeStreams.reduce((acc, streamVideo) => {
                if (!acc.has(streamVideo.userId)) {
                  acc.set(streamVideo.userId, streamVideo);
                }
                return acc;
              }, new Map<string, StreamVideo>()).values()
            ).map((streamVideo, idx) => (
              <div key={`user-${streamVideo.userId}`} style={{
                border: '2px solid #28a745',
                borderRadius: '8px',
                padding: '10px',
                backgroundColor: '#f8f9fa'
              }}>
                <h4>User: {streamVideo.userName || streamVideo.userId}</h4>
                <video
                  key={`video-${streamVideo.userId}`}
                  autoPlay
                  playsInline
                  muted={false}
                  controls
                  style={{
                    width: '100%',
                    height: 'auto',
                    borderRadius: '4px',
                  }}
                  ref={(videoElement) => {
                    if (videoElement) {
                      videoElement.srcObject = streamVideo.stream;
                      videoElement.play().catch(() => {});
                    }
                  }}
                />
                <p>Type: {streamVideo.kind}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {activeStreams.length === 0 && (
        <div style={{ 
          textAlign: 'center', 
          padding: '40px',
          backgroundColor: '#f8f9fa',
          borderRadius: '8px',
          border: '2px dashed #ccc'
        }}>
          <h3>No streams available</h3>
          <p>Waiting for users to start streaming...</p>
          <p style={{ fontSize: '14px', color: '#666' }}>
            Streams will automatically appear here when users join and start broadcasting.
          </p>
        </div>
      )}

      <div style={{ marginTop: '30px' }}>
        <h3 style={{ fontWeight: 'bold', color: '#333' }}>Auto-Streaming Instructions:</h3>
        <ol>
          <li>This page automatically connects and watches all available streams</li>
          <li>New streams automatically appear when users start broadcasting</li>
          <li>Streams automatically disappear when users leave</li>
          <li>No manual &quot;Watch&quot; buttons needed - everything is automatic!</li>
          <li>Use the toggle above to enable/disable auto-watching</li>
          <li>Share the stream link with others to join automatically</li>
        </ol>
      </div>
    </div>
  );
}
