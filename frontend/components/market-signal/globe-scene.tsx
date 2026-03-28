"use client"

import { useEffect, useRef, useState } from "react"
import * as THREE from "three"

export function GlobeCanvas() {
  const containerRef = useRef<HTMLDivElement>(null)
  const [isLoaded, setIsLoaded] = useState(false)
  const sceneRef = useRef<{
    scene: THREE.Scene
    camera: THREE.PerspectiveCamera
    renderer: THREE.WebGLRenderer
    globe: THREE.Mesh
    animationId: number
  } | null>(null)

  useEffect(() => {
    if (!containerRef.current) return

    // Scene setup
    const scene = new THREE.Scene()
    scene.background = new THREE.Color(0x09090b)

    // Camera
    const camera = new THREE.PerspectiveCamera(
      45,
      containerRef.current.clientWidth / containerRef.current.clientHeight,
      0.1,
      1000
    )
    camera.position.z = 3

    // Renderer
    const renderer = new THREE.WebGLRenderer({ antialias: true })
    renderer.setSize(containerRef.current.clientWidth, containerRef.current.clientHeight)
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
    containerRef.current.appendChild(renderer.domElement)

    // Globe geometry
    const geometry = new THREE.SphereGeometry(1, 64, 64)

    // Load Earth texture
    const textureLoader = new THREE.TextureLoader()
    const earthTexture = textureLoader.load(
      "/assets/3d/texture_earth.jpg",
      () => setIsLoaded(true),
      undefined,
      () => {
        // Fallback if texture fails to load - use a procedural texture
        setIsLoaded(true)
      }
    )

    // Material with Earth texture and green tint
    const material = new THREE.MeshPhongMaterial({
      map: earthTexture,
      color: 0x22c55e,
      emissive: 0x0a2010,
      emissiveIntensity: 0.3,
      shininess: 25,
    })

    const globe = new THREE.Mesh(geometry, material)
    scene.add(globe)

    // Wireframe overlay for grid effect
    const wireframeGeometry = new THREE.SphereGeometry(1.005, 32, 32)
    const wireframeMaterial = new THREE.MeshBasicMaterial({
      color: 0x22c55e,
      wireframe: true,
      transparent: true,
      opacity: 0.08,
    })
    const wireframe = new THREE.Mesh(wireframeGeometry, wireframeMaterial)
    scene.add(wireframe)

    // Atmosphere glow
    const atmosphereGeometry = new THREE.SphereGeometry(1.15, 64, 64)
    const atmosphereMaterial = new THREE.ShaderMaterial({
      vertexShader: `
        varying vec3 vNormal;
        void main() {
          vNormal = normalize(normalMatrix * normal);
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,
      fragmentShader: `
        varying vec3 vNormal;
        void main() {
          float intensity = pow(0.65 - dot(vNormal, vec3(0.0, 0.0, 1.0)), 2.0);
          gl_FragColor = vec4(0.133, 0.773, 0.369, 1.0) * intensity * 0.4;
        }
      `,
      blending: THREE.AdditiveBlending,
      side: THREE.BackSide,
      transparent: true,
    })
    const atmosphere = new THREE.Mesh(atmosphereGeometry, atmosphereMaterial)
    scene.add(atmosphere)

    // Lighting
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.3)
    scene.add(ambientLight)

    const directionalLight = new THREE.DirectionalLight(0xffffff, 1)
    directionalLight.position.set(5, 3, 5)
    scene.add(directionalLight)

    // Interaction state
    let isDragging = false
    let previousMousePosition = { x: 0, y: 0 }
    let rotationVelocity = { x: 0, y: 0 }

    // Mouse events
    const onMouseDown = (e: MouseEvent) => {
      isDragging = true
      previousMousePosition = { x: e.clientX, y: e.clientY }
    }

    const onMouseMove = (e: MouseEvent) => {
      if (!isDragging) return
      const deltaX = e.clientX - previousMousePosition.x
      const deltaY = e.clientY - previousMousePosition.y
      
      rotationVelocity.x = deltaY * 0.005
      rotationVelocity.y = deltaX * 0.005
      
      globe.rotation.x += rotationVelocity.x
      globe.rotation.y += rotationVelocity.y
      wireframe.rotation.x = globe.rotation.x
      wireframe.rotation.y = globe.rotation.y
      
      previousMousePosition = { x: e.clientX, y: e.clientY }
    }

    const onMouseUp = () => {
      isDragging = false
    }

    // Wheel zoom
    const onWheel = (e: WheelEvent) => {
      e.preventDefault()
      camera.position.z = Math.max(1.5, Math.min(5, camera.position.z + e.deltaY * 0.002))
    }

    containerRef.current.addEventListener("mousedown", onMouseDown)
    containerRef.current.addEventListener("mousemove", onMouseMove)
    containerRef.current.addEventListener("mouseup", onMouseUp)
    containerRef.current.addEventListener("mouseleave", onMouseUp)
    containerRef.current.addEventListener("wheel", onWheel, { passive: false })

    // Animation loop
    const animate = () => {
      const animationId = requestAnimationFrame(animate)
      
      // Auto-rotate when not dragging
      if (!isDragging) {
        globe.rotation.y += 0.002
        wireframe.rotation.y = globe.rotation.y
        
        // Apply friction to velocity
        rotationVelocity.x *= 0.95
        rotationVelocity.y *= 0.95
      }
      
      renderer.render(scene, camera)
      
      if (sceneRef.current) {
        sceneRef.current.animationId = animationId
      }
    }

    // Handle resize
    const handleResize = () => {
      if (!containerRef.current) return
      camera.aspect = containerRef.current.clientWidth / containerRef.current.clientHeight
      camera.updateProjectionMatrix()
      renderer.setSize(containerRef.current.clientWidth, containerRef.current.clientHeight)
    }
    window.addEventListener("resize", handleResize)

    // Store refs
    sceneRef.current = {
      scene,
      camera,
      renderer,
      globe,
      animationId: 0,
    }

    animate()

    // Cleanup
    return () => {
      window.removeEventListener("resize", handleResize)
      if (containerRef.current) {
        containerRef.current.removeEventListener("mousedown", onMouseDown)
        containerRef.current.removeEventListener("mousemove", onMouseMove)
        containerRef.current.removeEventListener("mouseup", onMouseUp)
        containerRef.current.removeEventListener("mouseleave", onMouseUp)
        containerRef.current.removeEventListener("wheel", onWheel)
      }
      if (sceneRef.current) {
        cancelAnimationFrame(sceneRef.current.animationId)
        sceneRef.current.renderer.dispose()
        if (containerRef.current && sceneRef.current.renderer.domElement.parentNode) {
          containerRef.current.removeChild(sceneRef.current.renderer.domElement)
        }
      }
    }
  }, [])

  return (
    <div className="absolute inset-0 pt-14 pb-0 bg-[#09090b]">
      {/* Loading state */}
      {!isLoaded && (
        <div className="absolute inset-0 flex items-center justify-center z-10">
          <div className="text-emerald-500 animate-pulse text-xs tracking-wide">
            Initializing globe...
          </div>
        </div>
      )}
      
      {/* Three.js canvas container */}
      <div 
        ref={containerRef} 
        className="w-full h-full cursor-grab active:cursor-grabbing"
      />

      {/* UI Elements */}
      <div className="absolute top-20 left-6 text-emerald-500 text-xs tracking-[0.25em] font-medium pointer-events-none">
        GLOBE
      </div>

      {/* Corner decorations */}
      <div className="absolute top-16 left-4 w-6 h-6 border-l border-t border-emerald-500/20 pointer-events-none" />
      <div className="absolute top-16 right-4 w-6 h-6 border-r border-t border-emerald-500/20 pointer-events-none" />
      <div className="absolute bottom-16 left-4 w-6 h-6 border-l border-b border-emerald-500/20 pointer-events-none" />
      <div className="absolute bottom-16 right-4 w-6 h-6 border-r border-b border-emerald-500/20 pointer-events-none" />

      {/* Instructions */}
      <div className="absolute bottom-4 left-1/2 -translate-x-1/2 text-[10px] text-emerald-500/40 tracking-widest pointer-events-none">
        DRAG TO ROTATE • SCROLL TO ZOOM
      </div>
    </div>
  )
}
