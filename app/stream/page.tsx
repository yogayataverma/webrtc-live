'use client';
import React, { useState, useEffect, useCallback } from 'react';
import io, { Socket } from 'socket.io-client';
import * as mediasoupClient from 'mediasoup-client';

type Producer = {
  id: string;
  kind: string;
  userId: string;
  userName?: string;
};

type StreamVideo = {
  producerId: string;
  kind: string;
  userId: string;
  userName: string;
  stream: MediaStream;
};

export default function Stream() {
  const [isStreaming, setIsStreaming] = useState(false);
  const [status, setStatus] = useState('Not connected');
  const [socket, setSocket] = useState<Socket | null>(null);
  const [availableProducers, setAvailableProducers] = useState<Producer[]>([]);
  const [activeStreams, setActiveStreams] = useState<StreamVideo[]>([]);
  const [device, setDevice] = useState<mediasoupClient.Device | null>(null);
  const [sendTransport, setSendTransport] = useState<mediasoupClient.types.Transport | null>(null);
  const [recvTransport, setRecvTransport] = useState<mediasoupClient.types.Transport | null>(null);
  const [userName, setUserName] = useState('');
  const [showNameInput, setShowNameInput] = useState(true);
  const [mySocketId, setMySocketId] = useState<string | null>(null);
  const [localStream, setLocalStream] = useState<MediaStream | null>(null);
  const [autoStreamStarted, setAutoStreamStarted] = useState(false);
  const [isInitialized, setIsInitialized] = useState(false);

  const consumeStream = useCallback(async (producer: Producer) => {
    if (!socket || !device || !recvTransport) {
      return;
    }

    if (producer.userId === socket.id) {
      return;
    }

    try {
      const consumerParams = await new Promise<{
        id: string;
        kind: string;
        rtpParameters: mediasoupClient.types.RtpParameters;
        producerId: string;
        error?: string;
      }>((resolve) => {
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
        kind: consumerParams.kind as 'audio' | 'video',
        rtpParameters: consumerParams.rtpParameters,
      });

      setActiveStreams(prevStreams => {
        const existingStreamIndex = prevStreams.findIndex(s => s.userId === producer.userId);
        
        if (existingStreamIndex !== -1) {
          const updatedStreams = [...prevStreams];
          updatedStreams[existingStreamIndex].stream.addTrack(consumer.track);
          
          updatedStreams[existingStreamIndex] = {
            ...updatedStreams[existingStreamIndex],
            kind: `${updatedStreams[existingStreamIndex].kind}+${producer.kind}`
          };
          return updatedStreams;
        } else {
          const newStream = new MediaStream();
          newStream.addTrack(consumer.track);
          
          const streamVideo: StreamVideo = {
            producerId: producer.id,
            kind: producer.kind,
            userId: producer.userId || 'unknown',
            userName: producer.userName || 'Unknown User',
            stream: newStream,
          };

          return [...prevStreams, streamVideo];
        }
      });

      setAvailableProducers(prev => prev.filter(p => p.id !== producer.id));

    } catch (error) {
      setStatus(`Error consuming stream: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [socket, device, recvTransport]);

  useEffect(() => {
    const initializeStream = async () => {
      try {
        const isSecure = window.location.protocol === 'https:' || 
                        window.location.hostname === 'webrtc-live.onrender.com' || 
                        window.location.hostname === '127.0.0.1';
                        
        if (!isSecure && navigator.mediaDevices) {
          setStatus('Warning: Media access may be limited on non-HTTPS connections');
        }
        
        setStatus('Connecting to server...');
        const socketConnection = io('https://webrtc-live.onrender.com');
        setSocket(socketConnection);

        socketConnection.on('connect', () => {
          setStatus('Connected to server');
          setMySocketId(socketConnection.id || null);
          
          socketConnection.emit('getAvailableProducers', (producers: Producer[]) => {
            const othersProducers = producers.filter(p => {
              const isOwn = p.userId === socketConnection.id;
              return !isOwn;
            });
            setAvailableProducers(othersProducers);
          });
        });

        socketConnection.on('disconnect', () => {
          setStatus('Disconnected from server');
          setIsStreaming(false);
          setActiveStreams([]);
        });

        socketConnection.on('newProducer', (data: { producerId: string; kind: string; userId: string; userName: string }) => {
          if (data.userId !== socketConnection.id) {
            setAvailableProducers(prev => [
              ...prev.filter(p => p.id !== data.producerId),
              { id: data.producerId, kind: data.kind, userId: data.userId, userName: data.userName }
            ]);
            
            if (isInitialized) {
              setTimeout(() => {
                const producer: Producer = {
                  id: data.producerId,
                  kind: data.kind,
                  userId: data.userId,
                  userName: data.userName
                };
                consumeStream(producer);
              }, 500);
            }
          }
        });

        socketConnection.on('userNameUpdated', (data: { userId: string; userName: string; producers: { id: string; kind: string }[] }) => {
          setAvailableProducers(prev => 
            prev.map(p => 
              data.producers.some(prod => prod.id === p.id) 
                ? { ...p, userName: data.userName }
                : p
            )
          );
          setActiveStreams(prev =>
            prev.map(stream =>
              stream.userId === data.userId
                ? { ...stream, userName: data.userName }
                : stream
            )
          );
        });

        socketConnection.on('producerClosed', (data: { producerId: string }) => {
          setAvailableProducers(prev => prev.filter(p => p.id !== data.producerId));
          setActiveStreams(prev => prev.filter(stream => stream.producerId !== data.producerId));
        });

        socketConnection.on('activeProducers', (producers: Producer[]) => {
          console.log('Received active producers:', producers);
          const othersProducers = producers.filter(p => p.userId !== socketConnection.id);
          setAvailableProducers(othersProducers);
        });

        const mediasoupDevice = new mediasoupClient.Device();
        
        const rtpCapabilities = await new Promise<mediasoupClient.types.RtpCapabilities>((resolve) => {
          socketConnection.emit('getRtpCapabilities', resolve);
        });

        await mediasoupDevice.load({ routerRtpCapabilities: rtpCapabilities });
        setDevice(mediasoupDevice);
        const recvTransportOptions = await new Promise<{
          id: string;
          iceParameters: mediasoupClient.types.IceParameters;
          iceCandidates: mediasoupClient.types.IceCandidate[];
          dtlsParameters: mediasoupClient.types.DtlsParameters;
          error?: string;
        }>((resolve) => {
          socketConnection.emit('createTransport', resolve);
        });

        if (!recvTransportOptions.error) {
          const recvTransportInstance = mediasoupDevice.createRecvTransport(recvTransportOptions);

          recvTransportInstance.on('connect', ({ dtlsParameters }, callback) => {
            socketConnection.emit('connectTransport', { 
              transportId: recvTransportOptions.id, 
              dtlsParameters 
            }, (response: { error?: string }) => {
              if (response.error) {
                callback();
              } else {
                callback();
              }
            });
          });

          setRecvTransport(recvTransportInstance);
        }

        setStatus('Requesting camera access...');
        
        if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
          throw new Error('Media devices not supported. Please ensure you are using HTTPS or localhost, and a modern browser that supports WebRTC.');
        }
        
        const stream = await navigator.mediaDevices.getUserMedia({ 
          audio: true, 
          video: { width: 640, height: 480 } 
        });
        
        setLocalStream(stream);
        
        const videoElement = document.getElementById('localVideo') as HTMLVideoElement;
        if (videoElement) {
          videoElement.srcObject = stream;
        }

        setStatus('Ready to stream and watch');
        setIsInitialized(true);
        
      } catch (error) {
        let errorMessage = 'Unknown error';
        if (error instanceof Error) {
          errorMessage = error.message;
          
          if (error.message.includes('getUserMedia')) {
            errorMessage = 'Camera/microphone access denied or not available. Please check permissions and ensure you\'re using HTTPS.';
          } else if (error.message.includes('Media devices not supported')) {
            errorMessage = 'Media devices not supported. Please use a modern browser with HTTPS.';
          } else if (error.message.includes('Permission denied')) {
            errorMessage = 'Camera/microphone permission denied. Please allow access and refresh the page.';
          }
        }
        
        setStatus(`Error: ${errorMessage}`);
      }
    };

    initializeStream();

    return () => {
      if (socket) {
        socket.disconnect();
      }
    };
  }, []);

  useEffect(() => {
    if (
      isInitialized &&
      availableProducers.length > 0 &&
      socket &&
      device &&
      recvTransport
    ) {
      availableProducers.forEach(producer => {
        setTimeout(() => {
          consumeStream(producer);
        }, 100);
      });
    }
  }, [isInitialized, availableProducers, consumeStream, socket, device, recvTransport]);

  useEffect(() => {
    if (localStream) {
      const videoElement = document.getElementById('localVideo') as HTMLVideoElement;
      if (videoElement && videoElement.srcObject !== localStream) {
        videoElement.srcObject = localStream;
      }
    }
  }, [localStream]);

  const startStreaming = async () => {
    if (!socket || !device) {
      setStatus('No socket connection or device not ready');
      return;
    }

    try {
      setStatus('Starting stream...');

      const transportOptions = await new Promise<{
        id: string;
        iceParameters: mediasoupClient.types.IceParameters;
        iceCandidates: mediasoupClient.types.IceCandidate[];
        dtlsParameters: mediasoupClient.types.DtlsParameters;
        error?: string;
      }>((resolve) => {
        socket.emit('createTransport', resolve);
      });

      if (transportOptions.error) {
        throw new Error(transportOptions.error);
      }

      const transport = device.createSendTransport(transportOptions);
      setSendTransport(transport);

      transport.on('connect', ({ dtlsParameters }, callback) => {
        socket.emit('connectTransport', { 
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

      transport.on('produce', ({ kind, rtpParameters }, callback) => {
        socket.emit('produce', { 
          transportId: transportOptions.id, 
          kind, 
          rtpParameters 
        }, (response: { id?: string; error?: string }) => {
          if (response.error) {
            console.error('Error in produce callback:', response.error);
          } else {
            console.log(`Successfully produced ${kind} track with ID: ${response.id}`);
            callback({ id: response.id! });
          }
        });
      });

      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        throw new Error('Media devices not supported. Please ensure you are using HTTPS or localhost, and a modern browser that supports WebRTC.');
      }
      
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: true, 
        video: { width: 640, height: 480 } 
      });

      console.log('Got user media with tracks:', stream.getTracks().map(t => `${t.kind}: ${t.readyState}`));

      const videoElement = document.getElementById('localVideo') as HTMLVideoElement;
      if (videoElement) {
        videoElement.srcObject = stream;
        console.log('Assigned stream to local video element');
      } else {
        console.error('Local video element not found!');
      }

      setLocalStream(stream);

      for (const track of stream.getTracks()) {
        console.log(`Starting to produce ${track.kind} track`);
        const producer = await transport.produce({ track });
        console.log(`Producer created for ${track.kind}:`, producer.id);
      }

      setIsStreaming(true);
      setStatus('Streaming live! You can now watch other streams below.');
    } catch (error) {
      console.error('Error starting stream:', error);
      setStatus(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const setUserNameOnServer = async (name: string) => {
    if (!socket) return;
    
    const response = await new Promise<{ success?: boolean; error?: string }>((resolve) => {
      socket.emit('setUserName', name, resolve);
    });
    
    if (response.success) {
      setUserName(name);
      setShowNameInput(false);
      setStatus(`Welcome, ${name}! Auto-starting stream...`);
      
      if (!autoStreamStarted) {
        setAutoStreamStarted(true);
        setTimeout(() => {
          startStreaming();
        }, 1000);
      }
    } else {
      console.error('Failed to set user name:', response.error);
    }
  };

  const stopStreaming = () => {
    if (sendTransport) {
      sendTransport.close();
      setSendTransport(null);
    }
    setIsStreaming(false);
    setStatus('Stream stopped');
  };

  return (
    <div style={{ padding: '20px', maxWidth: '1200px', margin: '0 auto' }}>
      <h1>Auto-Stream & Watch Videos</h1>
      
      {showNameInput && (
        <div style={{ 
          marginBottom: '20px', 
          padding: '20px', 
          backgroundColor: '#e3f2fd', 
          borderRadius: '8px',
          border: '2px solid #2196f3'
        }}>
          <h3>Enter Your Name</h3>
          <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
            <input
              type="text"
              placeholder="Enter your name..."
              value={userName}
              onChange={(e) => setUserName(e.target.value)}
              onKeyPress={(e) => {
                if (e.key === 'Enter' && userName.trim()) {
                  setUserNameOnServer(userName.trim());
                }
              }}
              style={{
                padding: '10px',
                fontSize: '16px',
                border: '1px solid #ccc',
                borderRadius: '4px',
                flex: 1,
                maxWidth: '300px'
              }}
            />
            <button
              onClick={() => userName.trim() && setUserNameOnServer(userName.trim())}
              disabled={!userName.trim()}
              style={{
                padding: '10px 20px',
                fontSize: '16px',
                backgroundColor: userName.trim() ? '#2196f3' : '#ccc',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: userName.trim() ? 'pointer' : 'not-allowed',
              }}
            >
              Set Name & Start Streaming
            </button>
          </div>
          <p style={{ margin: '10px 0 0 0', color: '#666', fontSize: '14px' }}>
            Choose a name that other users will see. Streaming will start automatically after setting your name.
          </p>
        </div>
      )}
      
      <div style={{ marginBottom: '20px' }}>
        <strong>Status:</strong> 
        <span style={{ 
          marginLeft: '10px',
          color: status.includes('Error') ? '#dc3545' : '#28a745' 
        }}>
          {status}
        </span>
      </div>

      <div style={{ marginBottom: '20px', fontSize: '14px', color: '#666' }}>
        <strong>Debug Info:</strong><br />
        Your Name: {userName || 'Not set'}<br />
        Connected: {socket ? 'Yes' : 'No'}<br />
        Streaming: {isStreaming ? 'Yes' : 'No'}<br />
        Available Producers: {availableProducers.length}<br />
        Active Streams: {activeStreams.length}
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
        {!showNameInput && (
          <div style={{ 
            padding: '20px', 
            backgroundColor: '#f8f9fa', 
            borderRadius: '8px',
            border: '2px solid #007bff'
          }}>
            <h2>Live Streams</h2>
            
            <div style={{ 
              display: 'flex', 
              gap: '20px', 
              flexWrap: 'wrap',
              justifyContent: 'flex-start'
            }}>
              <div style={{ 
                flex: '1 1 400px',
                minWidth: '350px',
                maxWidth: '500px',
                backgroundColor: '#e8f5e8',
                borderRadius: '8px',
                border: '2px solid #28a745',
                padding: '15px'
              }}>
                <h3 style={{ margin: '0 0 15px 0', color: '#28a745' }}>
                  Your Stream {userName && `(${userName})`}
                </h3>
                
                <video
                  id="localVideo"
                  autoPlay
                  muted
                  playsInline
                  style={{
                    width: '100%',
                    height: 'auto',
                    borderRadius: '8px',
                    border: '2px solid #28a745',
                    backgroundColor: '#000',
                    maxHeight: '300px'
                  }}
                />
                
                <div style={{ marginTop: '10px', textAlign: 'center' }}>
                  <p style={{ margin: '5px 0', fontWeight: 'bold', color: '#28a745' }}>
                    {isStreaming ? 'Broadcasting Live' : 'Auto-starting...'}
                  </p>
                  
                  <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '10px' }}>
                    {!isStreaming ? (
                      <button
                        onClick={startStreaming}
                        disabled={!socket || !device}
                        style={{
                          padding: '8px 16px',
                          fontSize: '14px',
                          backgroundColor: (!socket || !device) ? '#ccc' : '#007bff',
                          color: 'white',
                          border: 'none',
                          borderRadius: '4px',
                          cursor: (!socket || !device) ? 'not-allowed' : 'pointer',
                        }}
                      >
                        Manual Start
                      </button>
                    ) : (
                      <button
                        onClick={stopStreaming}
                        style={{
                          padding: '8px 16px',
                          fontSize: '14px',
                          backgroundColor: '#dc3545',
                          color: 'white',
                          border: 'none',
                          borderRadius: '4px',
                          cursor: 'pointer',
                        }}
                      >
                        Stop
                      </button>
                    )}
                  </div>
                </div>
              </div>
              {activeStreams.map((streamVideo) => (
                <div key={`${streamVideo.userId}-${streamVideo.producerId}`} style={{ 
                  flex: '1 1 400px',
                  minWidth: '350px',
                  maxWidth: '500px',
                  backgroundColor: '#fff3cd',
                  borderRadius: '8px',
                  border: '2px solid #ffc107',
                  padding: '15px'
                }}>
                  <h3 style={{ margin: '0 0 15px 0', color: '#856404' }}>
                    {streamVideo.userName}
                  </h3>
                  
                  <video
                    key={`video-${streamVideo.producerId}`}
                    autoPlay
                    playsInline
                    muted={false}
                    controls
                    style={{
                      width: '100%',
                      height: 'auto',
                      borderRadius: '8px',
                      border: '2px solid #ffc107',
                      backgroundColor: '#000',
                      maxHeight: '300px'
                    }}
                    ref={(videoElement) => {
                      if (videoElement) {
                        videoElement.srcObject = streamVideo.stream;
                        videoElement.play().catch(() => {});
                      }
                    }}
                  />
                  
                  <div style={{ marginTop: '10px', textAlign: 'center' }}>
                    <p style={{ margin: '5px 0', fontSize: '14px', color: '#856404' }}>
                     Live | {streamVideo.kind}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {availableProducers.length > 0 && (
            <div style={{ marginBottom: '20px' }}>
              <h3>Connecting to New Streams...</h3>
              <p style={{ fontSize: '12px', color: '#666', marginBottom: '10px' }}>
                These streams are being connected automatically and will appear above once ready.
              </p>
              
              {Object.entries(
                availableProducers.reduce((acc, producer) => {
                  const userName = producer.userName || 'Unknown User';
                  if (!acc[userName]) acc[userName] = [];
                  acc[userName].push(producer);
                  return acc;
                }, {} as Record<string, Producer[]>)
              ).map(([displayName, userProducers]) => (
                <div key={displayName} style={{ 
                  border: '2px solid #ffc107',
                  borderRadius: '8px',
                  padding: '15px',
                  backgroundColor: '#fff3cd',
                  marginBottom: '15px'
                }}>
                  <h4>{displayName}</h4>
                  <p>Connecting to: {userProducers.map(p => p.kind).join(', ')}</p>
                  
                  <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                    {userProducers.map((producer) => (
                      <button
                        key={producer.id}
                        onClick={() => consumeStream(producer)}
                        style={{
                          padding: '6px 12px',
                          backgroundColor: '#007bff',
                          color: 'white',
                          border: 'none',
                          borderRadius: '4px',
                          cursor: 'pointer',
                          fontSize: '12px',
                        }}
                      >
                        Force Connect {producer.kind}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}

        {availableProducers.length === 0 && activeStreams.length === 0 && (
          <div style={{ 
            textAlign: 'center', 
            padding: '20px',
            backgroundColor: '#f8f9fa',
            borderRadius: '8px',
            border: '2px dashed #ccc'
          }}>
            <h4>No other streams available</h4>
            <p>Waiting for other users to start streaming...</p>
            <p style={{ fontSize: '12px', color: '#666' }}>
              Open this page in another browser window/tab and set a different name to test!
            </p>
          </div>
        )}
      </div>

      <div style={{ marginTop: '30px' }}>
        <h3 style={{ fontWeight: 'bold', color: '#333' }}>How it works:</h3>
        <div style={{ marginTop: '20px', padding: '15px', backgroundColor: '#e8f5e8', borderRadius: '8px' }}>
          <ol>
            <li>Open this page in multiple browser windows/tabs</li>
            <li>Set different names in each window</li>
            <li>Watch streams appear automatically in all windows</li>
            <li>Close a window and see streams disappear from others</li>
          </ol>
        </div>
      </div>
    </div>
  );
}
