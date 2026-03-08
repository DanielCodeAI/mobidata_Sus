<template>
  <div id="app">
    <!-- Header -->
    <div class="header">
        <div>
            <h1 v-if="currentPage === 'routing'">🚌 Mein Profil & Präferenzen</h1>
            <h1 v-else>⚙️ CO₂-Konfiguration</h1>
            <p v-if="currentPage === 'routing'">Die Einstellungen werden genutzt für Suchergebnisse und CO₂-Kalkulation angewendet.</p>
            <p v-else>Passen Sie die CO₂-Emissionswerte für verschiedene Verkehrsmittel an.</p>
        </div>
        <button class="btn-header" @click="togglePage">
            <span v-if="currentPage === 'routing'">⚙️ CO₂-Konfiguration</span>
            <span v-else>← Zurück zur Routenplanung</span>
        </button>
    </div>

    <!-- Routing Page -->
    <div class="container" v-if="currentPage === 'routing'">
        <div class="grid-2col">
            <!-- Left Column: Transport Preferences -->
            <div class="card">
                <div class="card-header">Profil - Basispreferenzen für Verkehrsmittel</div>
                <div class="card-title">Bevorzugte Modi & Komfortdistanzen</div>
                
                <div class="card-subtitle">Ausgewählte Verkehrsmittel</div>
                <div class="transport-chips">
                    <div 
                        v-for="mode in transportModes" 
                        :key="mode.id"
                        :class="['chip', { active: mode.active }]"
                        @click="toggleMode(mode.id)"
                    >
                        {{ mode.name }}
                    </div>
                </div>

                <div class="card-header" style="margin-top: 24px;">Komfortdistanzen</div>
                <div class="distance-grid">
                    <div class="input-field">
                        <label>Angenehme Distanz zu Fuß</label>
                        <input type="number" v-model="preferences.walkDistance" step="0.5">
                        <div class="input-suffix">km</div>
                    </div>
                </div>
            </div>

            <!-- Right Column: Scoring -->
            <div class="card">
                <div class="card-header">Scoring - Gewichtung der Zielfunktionen</div>
                <div class="card-title">Zeit, CO₂ & Komfort ausbalancieren</div>

                <div class="slider-group">
                    <div class="slider-title">Zeit vs. Umwelt</div>
                    <div class="slider-description">
                        Wie wichtig ist Ihnen eine kurze Reisezeit im Vergleich zur CO₂-Einsparung?
                    </div>
                    <input type="range" min="0" max="100" v-model="scoring.timeVsCo2">
                    <div class="slider-labels">
                        <span>Hauptsache schnell</span>
                        <span class="center">{{ timeVsCo2Label }}</span>
                        <span>Hauptsache umwelt</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Route Planning Section -->
        <div class="card route-section">
            <div class="card-title">🗺️ Route planen</div>
            
            <div class="route-inputs">
                <div class="input-field">
                    <label>Von (Haltestelle)</label>
                    <input list="start-stops-list" v-model="startQuery" class="input-select" placeholder="Start eingeben..." />
                    <datalist id="start-stops-list">
                        <option v-for="stop in allStops" :key="stop.stop_id" :value="stop.stop_name"></option>
                    </datalist>
                </div>

                <div class="input-field">
                    <label>Nach (Haltestelle)</label>
                    <input list="end-stops-list" v-model="endQuery" class="input-select" placeholder="Ziel eingeben..." />
                    <datalist id="end-stops-list">
                        <option v-for="stop in allStops" :key="stop.stop_id" :value="stop.stop_name"></option>
                    </datalist>
                </div>
            </div>

            <button class="btn-primary" @click="findRoute" :disabled="loading">
                <span v-if="loading">⏳ Suche läuft...</span>
                <span v-else>🔍 Route suchen</span>
            </button>

            <div v-if="error" class="alert alert-error">{{ error }}</div>

            <div v-if="routeResult" class="route-result">
                <h3 style="font-size: 16px; margin-bottom: 16px;">📍 Ihre Route</h3>
                
                <div class="route-summary">
                    <div class="summary-card">
                        <div class="summary-value">{{ routeResult.summary.totalTime }}</div>
                        <div class="summary-label">Minuten</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">{{ routeResult.summary.totalDistance }}</div>
                        <div class="summary-label">Kilometer</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">{{ routeResult.summary.totalCo2 }}</div>
                        <div class="summary-label">g CO₂</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">{{ routeResult.summary.transfers }}</div>
                        <div class="summary-label">Umstiege</div>
                    </div>
                </div>

                <div class="route-step" v-for="(step, index) in routeResult.steps" :key="index">
                    <div class="route-icon">{{ step.line }}</div>
                    <div class="route-details">
                        <div class="route-from-to">{{ step.from }} → {{ step.to }}</div>
                        <div class="route-meta">
                            {{ step.time }} Min · {{ step.distance }} km · {{ step.co2 }} g CO₂
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Config Page -->
    <div class="container" v-if="currentPage === 'config'">
        <div class="card">
            <div v-if="configSaved" class="alert alert-success">
                ✓ Konfiguration erfolgreich gespeichert! (Diese Werte werden nun als Basis-Gewichtungen genutzt)
            </div>

            <div class="card-title">CO₂-Emissionen pro Verkehrsmittel (g/km)</div>
            <div class="card-subtitle">Diese Werte werden für die Routenberechnung und CO₂-Bilanz verwendet.</div>

            <div class="info-box">
                <h3>💡 Hinweis</h3>
                <ul>
                    <li>Standard-Werte: Tram=40, U-Bahn=30, Zug=35, Bus=80</li>
                    <li>Änderungen wirken sich auf den Routing-Algorithmus aus.</li>
                </ul>
            </div>

            <div class="config-grid">
                <div class="config-label">
                    <strong>Tram / Straßenbahn</strong>
                </div>
                <div class="config-input">
                    <input type="number" v-model.number="co2Config.tram" min="0">
                    <span style="font-size: 13px; color: #666;">g/km</span>
                </div>
            </div>

            <div class="config-grid">
                <div class="config-label">
                    <strong>U-Bahn</strong>
                </div>
                <div class="config-input">
                    <input type="number" v-model.number="co2Config.subway" min="0">
                    <span style="font-size: 13px; color: #666;">g/km</span>
                </div>
            </div>

            <div class="config-grid">
                <div class="config-label">
                    <strong>Zug / Bahn</strong>
                </div>
                <div class="config-input">
                    <input type="number" v-model.number="co2Config.rail" min="0">
                    <span style="font-size: 13px; color: #666;">g/km</span>
                </div>
            </div>

            <div class="config-grid">
                <div class="config-label">
                    <strong>Bus</strong>
                </div>
                <div class="config-input">
                    <input type="number" v-model.number="co2Config.bus" min="0">
                    <span style="font-size: 13px; color: #666;">g/km</span>
                </div>
            </div>

            <button class="btn-primary" @click="saveConfig" style="margin-top: 24px;">
                <span>💾 Konfiguration speichern</span>
            </button>
        </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'

const currentPage = ref('routing')
const loading = ref(false)
const error = ref('')
const configSaved = ref(false)

const co2Config = ref({
    tram: 40,
    subway: 30,
    rail: 35,
    bus: 80
})

const saveConfig = () => {
    configSaved.value = true
    setTimeout(() => { configSaved.value = false }, 3000)
}

// Preferences
const transportModes = ref([
    { id: 'walk', name: 'Zu Fuß', active: true },
    { id: 'bike', name: 'Fahrrad', active: true },
    { id: 'opnv', name: 'ÖPNV', active: true }
])
const preferences = ref({
    walkDistance: 2.5
})
const scoring = ref({
    timeVsCo2: 50
})

const timeVsCo2Label = computed(() => {
    return scoring.value.timeVsCo2 + '%'
})

// Routing
const allStops = ref([])
const startQuery = ref('')
const endQuery = ref('')
const routeResult = ref(null)

const togglePage = () => {
    currentPage.value = currentPage.value === 'routing' ? 'config' : 'routing'
}
const toggleMode = (id) => {
    const mode = transportModes.value.find(m => m.id === id)
    if (mode) mode.active = !mode.active
}

const loadStops = async () => {
    try {
        const res = await fetch('http://localhost:8000/stops')
        const data = await res.json()
        allStops.value = data
    } catch (err) {
        console.error("Failed to load stops", err)
    }
}

const findRoute = async () => {
    if (!startQuery.value || !endQuery.value) {
        error.value = "Bitte Start und Ziel wählen."
        return
    }
    
    loading.value = true
    error.value = ''
    routeResult.value = null

    const startStop = allStops.value.find(s => s.stop_name === startQuery.value)
    const endStop = allStops.value.find(s => s.stop_name === endQuery.value)
    
    if (!startStop || !endStop) {
        error.value = "Start oder Ziel nicht gefunden. Bitte aus der Liste wählen."
        loading.value = false
        return
    }

    try {
        const payload = {
            start_stop_id: startStop.stop_id,
            end_stop_id: endStop.stop_id,
            time_vs_co2_weight: scoring.value.timeVsCo2 / 100.0,
            algorithm: 'dijkstra',
            co2_config: co2Config.value
        }
        
        const res = await fetch('http://localhost:8000/route', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        
        const data = await res.json()
        if (data.error) {
            error.value = data.error
        } else {
            routeResult.value = data
        }
    } catch (err) {
        error.value = "Server Fehler bei der Routenberechnung."
        console.error(err)
    } finally {
        loading.value = false
    }
}

onMounted(() => {
    loadStops()
})
</script>

<style>
/* Base Styles matching the HTML prototype */
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; min-height: 100vh; }
.header { background: linear-gradient(135deg, #4F7FFF 0%, #00D4AA 100%); padding: 24px 40px; color: white; display: flex; justify-content: space-between; align-items: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
.header h1 { font-size: 20px; font-weight: 600; }
.header p { font-size: 13px; opacity: 0.9; margin-top: 4px; }
.btn-header { background: white; color: #4F7FFF; border: none; padding: 10px 20px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 14px; transition: all 0.2s; }
.btn-header:hover { background: #f0f0f0; transform: translateY(-1px); }
.container { max-width: 1400px; margin: 0 auto; padding: 32px 40px; }
.grid-2col { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }
.card { background: white; border-radius: 12px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.card-header { font-size: 11px; font-weight: 600; color: #666; text-transform: uppercase; margin-bottom: 12px; }
.card-title { font-size: 18px; font-weight: 600; color: #1a1a1a; margin-bottom: 16px; }
.card-subtitle { font-size: 13px; color: #666; margin-bottom: 20px; }
.transport-chips { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 24px; }
.chip { padding: 6px 14px; border-radius: 16px; font-size: 13px; font-weight: 500; border: 1.5px solid #e0e0e0; background: white; cursor: pointer; transition: all 0.2s; }
.chip.active { background: #4F7FFF; color: white; border-color: #4F7FFF; }
.input-select { width: 100%; padding: 10px 12px; border: 1.5px solid #e0e0e0; border-radius: 6px; font-size: 14px; }
.distance-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
.input-field { margin-bottom: 16px; }
.input-field label { display: block; font-size: 13px; color: #666; margin-bottom: 6px; }
.input-field input { width: 100%; padding: 10px 12px; border: 1.5px solid #e0e0e0; border-radius: 6px; font-size: 14px; }
.slider-group { margin-bottom: 28px; }
.slider-title { font-size: 14px; font-weight: 600; color: #1a1a1a; margin-bottom: 8px; }
.slider-description { font-size: 13px; color: #666; margin-bottom: 16px; }
input[type="range"] { width: 100%; height: 6px; border-radius: 3px; background: linear-gradient(to right, #4F7FFF 0%, #00D4AA 100%); outline: none; -webkit-appearance: none; margin: 8px 0; }
input[type="range"]::-webkit-slider-thumb { -webkit-appearance: none; width: 20px; height: 20px; border-radius: 50%; background: white; cursor: pointer; border: 3px solid #4F7FFF; box-shadow: 0 2px 6px rgba(0,0,0,0.15); }
.slider-labels { display: flex; justify-content: space-between; font-size: 12px; color: #999; margin-top: 8px; }
.route-section { margin-top: 24px; }
.route-inputs { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
.btn-primary { width: 100%; padding: 14px; background: linear-gradient(135deg, #4F7FFF 0%, #00D4AA 100%); color: white; border: none; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; transition: all 0.2s; box-shadow: 0 2px 8px rgba(79, 127, 255, 0.25); }
.alert { padding: 14px 16px; border-radius: 8px; margin-top: 16px; font-size: 14px; }
.alert-error { background: #fee; color: #c33; border: 1px solid #fcc; }
.route-result { margin-top: 24px; padding: 20px; background: #f8f9fa; border-radius: 8px; }
.route-summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
.summary-card { background: white; padding: 16px; border-radius: 8px; text-align: center; }
.summary-value { font-size: 24px; font-weight: 700; color: #4F7FFF; }
.summary-label { font-size: 12px; color: #666; margin-top: 4px; }
.route-step { background: white; padding: 16px; border-radius: 8px; margin-bottom: 12px; display: flex; align-items: center; gap: 16px; }
.route-icon { min-width: 44px; height: 44px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 13px; color: white; background: #4F7FFF; }
.route-details { flex: 1; }
.route-from-to { font-size: 14px; font-weight: 500; color: #1a1a1a; margin-bottom: 4px; }
.route-meta { font-size: 12px; color: #999; }

.config-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; padding: 16px 0; border-bottom: 1px solid #f0f0f0; }
.config-grid:last-child { border-bottom: none; }
.config-label strong { font-size: 14px; color: #1a1a1a; }
.config-input { display: flex; align-items: center; gap: 10px; }
.config-input input { flex: 1; padding: 10px 12px; border: 1.5px solid #e0e0e0; border-radius: 6px; font-size: 14px; }
.info-box { background: #e3f2fd; border-left: 4px solid #2196f3; padding: 16px; border-radius: 6px; margin-bottom: 24px; }
.info-box h3 { font-size: 14px; font-weight: 600; color: #1565c0; margin-bottom: 8px; }
.info-box ul { margin-left: 20px; font-size: 13px; color: #555; }
.alert-success { background: #efe; color: #3a3; border: 1px solid #cfc; }
</style>
