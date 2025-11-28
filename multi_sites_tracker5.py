#!/usr/bin/env python3
"""
Tracker multi-sites - VERSION FINALE AVEC CAPTURE CONTINUE + RÉSULTATS AUTOMATIQUES
─────────────────────────────────────────────────────────────────────────────────
Fonctionnalités :
├─ externalId au lieu de betRadarId (identifiant unique)
├─ GetSport avec pagination parallèle (tous les matchs du jour)
├─ Capture dynamique toutes les 2 minutes (matchs proches)
├─ CAPTURE CONTINUE pour matchs < 60 min (mise à jour cotes)
├─ DÉTECTION CHANGEMENTS de cotes (seuil > 0.01)
├─ SYSTÈME DE RETRY pour matchs sans cotes (max 5 tentatives)
├─ Capture IMMÉDIATE avant disparition (même 9h avant)
├─ Optimisation quotas Google Sheets (cache + retry)
├─ Resynchronisation Excel → Google Sheets
├─ append_rows() au lieu de batch_update() (fix désynchronisation)
├─ Gestion matchs sans cotes (évite boucle infinie)
├─ Surveillance multi-niveau (< 15 min, 15-60 min, > 60 min)
├─ File d'attente avec priorités
├─ Nom compétition depuis GetMatch
├─ Finalisation matchs déjà commencés
├─ 4 sites en parallèle
├─ Excel local + Google Sheets
├─ Fuseau horaire Maurice (UTC+4)
├─ PERSISTENCE DU CACHE (sauvegarde/restauration sur disque)
├─ ANTI DOUBLE-FINALISATION (dédup queue + verrou en cours)
├─ RÉCUPÉRATION AUTOMATIQUE DES RÉSULTATS (3h après fin match)
├─ API searchresult + GetMatchData (4 sites en parallèle)
├─ Score et Résultat par marché (FT/HT/2H)
├─ Colonnes métadonnées cachées automatiquement

Auteur: antema102
Date: 2025-11-28
Version: 2.5 (résultats automatiques)
"""
import sys
import asyncio

# FIX WINDOWS
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path
from collections import defaultdict, deque
import traceback
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser as date_parser
import pickle  # Persistence

# Configuration fuseau horaire Maurice
MAURITIUS_TZ = ZoneInfo("Indian/Mauritius")

def now_mauritius():
    return datetime.now(MAURITIUS_TZ)

def now_mauritius_iso():
    return datetime.now(MAURITIUS_TZ).isoformat()

def now_mauritius_str(fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now(MAURITIUS_TZ).strftime(fmt)

def now_mauritius_date_str():
    """Retourne la date actuelle à Maurice (format ISO pour API)"""
    return datetime.now(MAURITIUS_TZ).strftime("%Y-%m-%d")

# Configuration sites
SITES = {
    "stevenhills": {"name": "StevenHills", "base_url": "https://stevenhills.bet"},
    "superscore": {"name": "SuperScore", "base_url": "https://superscore.mu"},
    "totelepep": {"name": "ToteLePEP", "base_url": "https://www.totelepep.mu"},
    "playonlineltd": {"name": "PlayOnline", "base_url": "http://playonlineltd.net"}
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://stevenhills.bet/",
}

TRACKING_INTERVAL_SECONDS = 120
TIMEOUT = 20
MAX_RETRIES = 3

# Fenêtres temporelles
INTENSIVE_CAPTURE_WINDOW_MINUTES = 15
CLOSE_MATCH_WINDOW_MINUTES = 60
FINALIZATION_WINDOW_MINUTES = 10

# Configuration Google Sheets
GOOGLE_SHEETS_CONFIG = {
    "credentials_file": "eternal-outlook-441811-k7-9e7b056d27b2.json",
    "sheet_name": "ALL_MATCHES",
}

# Mapping marchés
MARKET_SHEET_MAPPING = {
    "CP_FT": "1X2_FullTime",
    "CP_H1": "1X2_HalfTime",
    "CP_H2": "1X2_2ndHalf",
    "UO_FT_+1.5": "OverUnder_1.5_FT",
    "UO_FT_+2.5": "OverUnder_2.5_FT",
    "UO_FT_+3.5": "OverUnder_3.5_FT",
    "UO_H1_+1.5": "OverUnder_1.5_HT",
    "UO_H1_+2.5": "OverUnder_2.5_HT",
    "UO_H2_+1.5": "OverUnder_1.5_2H",
    "UO_H2_+2.5": "UO_H2_+2.5",
    "DC_FT": "DoubleChance_FT",
    "DC_H1": "DoubleChance_HT",
    "DC_H2": "DoubleChance_2H",
    "BT_FT": "BothTeamsToScore_FT",
    "BT_H1": "BothTeamsToScore_HT",
    "BT_H2": "BothTeamsToScore_2H",
    "CS_FT": "CorrectScore_FT",
    "CS_H1": "CorrectScore_HT",
    "CS_H2": "CorrectScore_2H",
    "OE_FT": "OddEven_FT",
    "OE_H1": "OE_H1",
    "OE_H2": "OE_H2",
    "HF_FT": "HalfTimeFullTime",
    "DB_FT": "DrawNoBet_FT",
    "GM_FT": "GoalMarket_FT",
    "GM_H1": "GoalMarket_HT",
    "GM_H2": "GoalMarket_2H",
    "WM_FT": "WinningMargin_FT",
    "FS_FT": "FirstToScore_FT",
    "LS_FT": "LastToScore_FT",
    "HSH_FT": "HighestScoringHalf_FT",
    "HSHA_FT": "HSHA_FT",
    "HSHH_FT": "HSHH_FT",
}

# Mapping Score selon le marché (FT/HT/2H)
SCORE_MAPPING = {
    # Markets FullTime → Score FT
    "CP_FT": "FT",
    "UO_FT_+1.5": "FT",
    "UO_FT_+2.5": "FT",
    "UO_FT_+3.5": "FT",
    "DC_FT": "FT",
    "BT_FT": "FT",
    "CS_FT": "FT",
    "OE_FT": "FT",
    "HF_FT": "FT",
    "DB_FT": "FT",
    "GM_FT": "FT",
    "WM_FT": "FT",
    "FS_FT": "FT",
    "LS_FT": "FT",
    "HSH_FT": "FT",
    "HSHA_FT": "FT",
    "HSHH_FT": "FT",
    
    # Markets HalfTime → Score HT
    "CP_H1": "HT",
    "UO_H1_+1.5": "HT",
    "UO_H1_+2.5": "HT",
    "DC_H1": "HT",
    "BT_H1": "HT",
    "CS_H1": "HT",
    "OE_H1": "HT",
    "GM_H1": "HT",
    
    # Markets 2nd Half → Score 2H
    "CP_H2": "2H",
    "UO_H2_+1.5": "2H",
    "UO_H2_+2.5": "2H",
    "DC_H2": "2H",
    "BT_H2": "2H",
    "CS_H2": "2H",
    "OE_H2": "2H",
    "GM_H2": "2H",
}

# Configuration résultats automatiques
RESULTS_CHECK_INTERVAL_HOURS = 1        # Check toutes les heures
RESULTS_DELAY_HOURS = 3                 # Attendre 3h après fin match
MAX_RESULTS_RETRY = 5                   # Max 5 tentatives


class MatchFinalizationQueue:
    """File d'attente pour les matchs à finaliser avec système de priorités"""
    
    def __init__(self, batch_size: int = 5, min_interval_seconds: int = 20):
        self.queue = deque()
        self.processing = False
        self.last_batch_time = None
        
        self.batch_size = batch_size
        self.min_interval_seconds = min_interval_seconds

        # Anti-doublon dans la file
        self.queued_ids: Set[int] = set()
    
    def add_match(self, external_id: int, priority: str = "normal"):
        """Ajouter un match à la queue (anti-doublon + upgrade priorité)"""
        # Si déjà en file, upgrade éventuelle de priorité
        if external_id in self.queued_ids:
            for item in self.queue:
                if item["external_id"] == external_id:
                    if priority == "urgent" and item["priority"] == "normal":
                        item["priority"] = "urgent"
            return
        
        self.queue.append({
            "external_id": external_id,
            "priority": priority,
            "added_at": now_mauritius()
        })
        self.queued_ids.add(external_id)
    
    def get_next_batch(self) -> List[int]:
        """Récupérer le prochain batch de matchs"""
        if not self.queue:
            return []
        
        if self.last_batch_time:
            elapsed = (now_mauritius() - self.last_batch_time).total_seconds()
            if elapsed < self.min_interval_seconds:
                return []
        
        urgent = [item for item in self.queue if item["priority"] == "urgent"]
        normal = [item for item in self.queue if item["priority"] == "normal"]
        
        batch_items = (urgent + normal)[:self.batch_size]
        batch_ids = [item["external_id"] for item in batch_items]
        
        for item in batch_items:
            self.queue.remove(item)
            self.queued_ids.discard(item["external_id"])
        
        self.last_batch_time = now_mauritius()
        
        return batch_ids
    
    def __contains__(self, external_id: int) -> bool:
        return external_id in self.queued_ids

    def __len__(self):
        return len(self.queue)


class MatchResultsQueue:
    """File d'attente pour récupération résultats avec délai"""
    
    def __init__(self, delay_hours: int = 3, max_retries: int = 5):
        """
        Initialiser la queue de résultats.
        
        Args:
            delay_hours: Nombre d'heures à attendre après fin estimée du match
            max_retries: Nombre maximum de tentatives par match
        """
        self.queue = deque()
        self.delay_hours = delay_hours
        self.max_retries = max_retries
        self.queued_ids: Set[int] = set()
    
    def add_match(self, external_id: int, match_info: dict, estimated_end_time: datetime):
        """
        Ajouter un match à la queue (disponible après delay_hours).
        
        Args:
            external_id: ID externe unique du match
            match_info: Informations du match (métadonnées par site)
            estimated_end_time: Heure de fin estimée du match
        """
        if external_id in self.queued_ids:
            return  # Déjà en queue
        
        ready_time = estimated_end_time + timedelta(hours=self.delay_hours)
        
        self.queue.append({
            "external_id": external_id,
            "match_info": match_info,
            "estimated_end_time": estimated_end_time,
            "ready_time": ready_time,
            "added_at": now_mauritius(),
            "retry_count": 0,
            "last_attempt": None
        })
        self.queued_ids.add(external_id)
    
    def get_ready_matches(self) -> List[dict]:
        """
        Récupérer les matchs prêts (> delay_hours après fin estimée).
        
        Returns:
            Liste des matchs dont les résultats peuvent être récupérés
        """
        now = now_mauritius()
        ready_matches = []
        
        for item in list(self.queue):
            if now >= item["ready_time"]:
                # Vérifier si pas trop de retries
                if item["retry_count"] < self.max_retries:
                    ready_matches.append(item)
        
        return ready_matches
    
    def mark_completed(self, external_id: int):
        """
        Marquer un match comme traité (résultats récupérés).
        
        Args:
            external_id: ID externe du match
        """
        self.queue = deque([item for item in self.queue if item["external_id"] != external_id])
        self.queued_ids.discard(external_id)
    
    def increment_retry(self, external_id: int) -> bool:
        """
        Incrémenter le compteur de retry et mettre à jour ready_time.
        
        Args:
            external_id: ID externe du match
            
        Returns:
            True si encore des tentatives possibles, False si max atteint
        """
        for item in self.queue:
            if item["external_id"] == external_id:
                item["retry_count"] += 1
                item["last_attempt"] = now_mauritius()
                # Prochain retry dans 1 heure
                item["ready_time"] = now_mauritius() + timedelta(hours=1)
                
                if item["retry_count"] >= self.max_retries:
                    return False
                return True
        return False
    
    def mark_failed(self, external_id: int):
        """
        Marquer un match comme définitivement échoué (max retries atteint).
        
        Args:
            external_id: ID externe du match
        """
        self.queue = deque([item for item in self.queue if item["external_id"] != external_id])
        self.queued_ids.discard(external_id)
    
    def __contains__(self, external_id: int) -> bool:
        return external_id in self.queued_ids
    
    def __len__(self):
        return len(self.queue)
    
    def to_serializable(self) -> List[dict]:
        """Convertir en format sérialisable pour pickle."""
        result = []
        for item in self.queue:
            serializable_item = item.copy()
            # Convertir datetime en string
            for key in ["estimated_end_time", "ready_time", "added_at", "last_attempt"]:
                if serializable_item.get(key) and isinstance(serializable_item[key], datetime):
                    serializable_item[key] = serializable_item[key].isoformat()
            result.append(serializable_item)
        return result
    
    def from_serializable(self, data: List[dict]):
        """Restaurer depuis format sérialisable."""
        self.queue.clear()
        self.queued_ids.clear()
        
        for item in data:
            # Convertir string en datetime
            for key in ["estimated_end_time", "ready_time", "added_at", "last_attempt"]:
                if item.get(key) and isinstance(item[key], str):
                    try:
                        dt = date_parser.parse(item[key])
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=MAURITIUS_TZ)
                        item[key] = dt
                    except:
                        item[key] = None
            
            self.queue.append(item)
            self.queued_ids.add(item["external_id"])


class APIHealthMonitor:
    """Moniteur de santé des API GetSport et GetMatch"""
    
    def __init__(self):
        self.stats = {
            "getsport": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []}),
            "getmatch": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []})
        }
        self.last_reset = now_mauritius()
    
    def record_getsport(self, site_key: str, success: bool, error_msg: str = ""):
        """Enregistrer un appel GetSport"""
        if success:
            self.stats["getsport"][site_key]["success"] += 1
        else:
            self.stats["getsport"][site_key]["failed"] += 1
            if error_msg:
                self.stats["getsport"][site_key]["errors"].append({
                    "time": now_mauritius_str("%H:%M:%S"),
                    "error": error_msg
                })
    
    def record_getmatch(self, site_key: str, success: bool, error_msg: str = ""):
        """Enregistrer un appel GetMatch"""
        if success:
            self.stats["getmatch"][site_key]["success"] += 1
        else:
            self.stats["getmatch"][site_key]["failed"] += 1
            if error_msg:
                # Limiter à 5 dernières erreurs
                if len(self.stats["getmatch"][site_key]["errors"]) >= 5:
                    self.stats["getmatch"][site_key]["errors"].pop(0)
                self.stats["getmatch"][site_key]["errors"].append({
                    "time": now_mauritius_str("%H:%M:%S"),
                    "error": error_msg
                })
    
    def get_health_score(self, site_key: str) -> float:
        """Calculer score de santé (0-100)"""
        total_success = (
            self.stats["getsport"][site_key]["success"] + 
            self.stats["getmatch"][site_key]["success"]
        )
        total_failed = (
            self.stats["getsport"][site_key]["failed"] + 
            self.stats["getmatch"][site_key]["failed"]
        )
        
        total = total_success + total_failed
        if total == 0:
            return 100.0
        
        return (total_success / total) * 100
    
    def print_report(self):
        """Afficher rapport de santé"""
        print(f"\n   🏥 Santé API (dernière heure) :")
        
        for site_key in SITES.keys():
            score = self.get_health_score(site_key)
            
            if score >= 95:
                emoji = "🟢"
            elif score >= 80:
                emoji = "🟡"
            elif score >= 50:
                emoji = "🟠"
            else:
                emoji = "🔴"
            
            gs_success = self.stats["getsport"][site_key]["success"]
            gs_failed = self.stats["getsport"][site_key]["failed"]
            gm_success = self.stats["getmatch"][site_key]["success"]
            gm_failed = self.stats["getmatch"][site_key]["failed"]
            
            gs_total = gs_success + gs_failed
            gm_total = gm_success + gm_failed
            
            print(f"      {emoji} {SITES[site_key]['name']:15s}: {score:5.1f}% ", end="")
            
            if gs_total > 0 or gm_total > 0:
                print(f"(GetSport: {gs_success}/{gs_total}, GetMatch: {gm_success}/{gm_total})")
            else:
                print("(Aucune tentative)")
            
            # Afficher dernières erreurs si score faible
            if score < 90 and (gs_failed > 0 or gm_failed > 0):
                recent_errors = (
                    self.stats["getsport"][site_key]["errors"][-2:] + 
                    self.stats["getmatch"][site_key]["errors"][-2:]
                )
                for err in recent_errors:
                    print(f"         └─ {err['time']}: {err['error']}")
    
    def reset_if_needed(self):
        """Reset toutes les heures"""
        if (now_mauritius() - self.last_reset).total_seconds() > 3600:
            print(f"\n   🔄 Statistiques API réinitialisées (nouvelle heure)")
            self.stats = {
                "getsport": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []}),
                "getmatch": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []})
            }
            self.last_reset = now_mauritius()


class GoogleSheetsManager:
    """Gestionnaire Google Sheets OPTIMISÉ avec append au lieu de batch_update"""
    
    def __init__(self, credentials_file: str, sheet_name: str):
        self.credentials_file = credentials_file
        self.sheet_name = sheet_name
        self.client = None
        self.spreadsheet = None
        self.worksheets = {}
        self.headers_initialized = set()
        
        self.last_row_cache = {}
        
        self.last_api_call = None
        self.min_interval_between_calls = 1.0
        
        self.api_call_count = 0
        self.api_call_window_start = time.time()
        self._last_summary_update = 0
        
    def connect(self):
        """Se connecter à Google Sheets"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file, scope
            )
            
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(self.sheet_name)
            
            print(f"✅ Connecté à Google Sheets: {self.sheet_name}")
            return True
            
        except FileNotFoundError:
            print(f"❌ Fichier credentials non trouvé: {self.credentials_file}")
            return False
        
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"❌ Google Sheet non trouvé: {self.sheet_name}")
            return False
        
        except Exception as e:
            print(f"❌ Erreur connexion Google Sheets: {e}")
            return False
                
    def _track_api_call(self):
        """Tracker avec affichage en temps réel"""
        now = time.time()
        
        if now - self.api_call_window_start > 60:
            if self.api_call_count > 0:
                print(f"   📊 Dernière minute : {self.api_call_count}/60 appels")
            self.api_call_count = 0
            self.api_call_window_start = now
        
        self.api_call_count += 1
        
        # AFFICHER TOUS LES 10 APPELS
        if self.api_call_count % 10 == 0:
            print(f"         📊 {self.api_call_count}/60 appels API...")
        
        # RALENTIR AUTOMATIQUEMENT
        if self.api_call_count >= 45:
            print(f"         ⚠️  Quota élevé ({self.api_call_count}/60) → ralentissement")
            self.min_interval_between_calls = 3.0
        elif self.api_call_count >= 50:
            print(f"         🚨 QUOTA CRITIQUE ({self.api_call_count}/60) → pause forcée")
            time.sleep(5)
    
    def _wait_if_needed(self):
        """Attendre si nécessaire pour respecter le rate limit"""
        self._track_api_call()
        
        if self.last_api_call is None:
            self.last_api_call = time.time()
            return
        
        elapsed = time.time() - self.last_api_call
        
        if elapsed < self.min_interval_between_calls:
            wait_time = self.min_interval_between_calls - elapsed
            time.sleep(wait_time)
        
        self.last_api_call = time.time()

    def _execute_with_retry(self, func, max_retries=2, backoff_factor=3):
        """Exécuter avec retry réduit"""
        for attempt in range(max_retries):
            try:
                self._wait_if_needed()
                
                # TIMEOUT via gspread
                import socket
                original_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(30)  # 30s timeout
                
                try:
                    result = func()
                    return result
                finally:
                    socket.setdefaulttimeout(original_timeout)
                
            except (gspread.exceptions.APIError, socket.timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** (attempt + 1)
                    print(f"         ⏸️  Erreur API → Attente {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"         ❌ Échec après {max_retries} tentatives")
                    raise

    def get_or_create_worksheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """Obtenir ou créer une feuille"""
        try:
            if sheet_name in self.worksheets:
                return self.worksheets[sheet_name]
            
            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=1000, 
                    cols=20
                )
                print(f"   📄 Nouvelle feuille créée: {sheet_name}")
            
            self.worksheets[sheet_name] = worksheet
            return worksheet
            
        except Exception as e:
            print(f"   ⚠️  Erreur feuille {sheet_name}: {e}")
            return None
    
    def _ensure_header(self, worksheet: gspread.Worksheet, sheet_name: str, expected_header: List[str]) -> bool:
        """Garantir que le header est correct"""
        try:
            if sheet_name in self.headers_initialized:
                return True
            
            try:
                first_row = worksheet.row_values(1)
            except:
                first_row = []
            
            if not first_row or all(not cell for cell in first_row):
                def update_header():
                    return worksheet.update(values=[expected_header], range_name='A1')
                self._execute_with_retry(update_header)
                self.headers_initialized.add(sheet_name)
                return True
            
            if first_row == expected_header:
                self.headers_initialized.add(sheet_name)
                return True
            
            self.headers_initialized.add(sheet_name)
            return True
            
        except Exception as e:
            return False
    
    def invalidate_cache(self):
        """Invalider le cache"""
        if self.last_row_cache:
            print(f"   🧹 Cache Google Sheets invalidé ({len(self.last_row_cache)} feuilles)")
        self.last_row_cache.clear()
       
    async def append_rows_batch(self, sheets_data: Dict[str, List[Dict]], sheets_to_hide_cols: Set[str] = None):
        """
        Utiliser append() avec pauses adaptatives.
        
        Args:
            sheets_data: Dict {sheet_name: [row_dicts]}
            sheets_to_hide_cols: Set des noms de feuilles où masquer les colonnes métadonnées
        """
        try:
            print(f"\n      📤 Préparation batch ({len(sheets_data)} feuilles)...")
            
            total_rows_sent = 0
            feuilles_traitees = 0
            
            # Déterminer l'index de début des colonnes métadonnées
            # Colonnes: Date, Heure, External ID, Match, Compétition, Heure Match, 
            #           StevenHills, SuperScore, ToteLePEP, PlayOnline, Score, Résultat,
            #           MatchId_SH, CompId_SH, SportId_SH, ... (12 colonnes métadonnées)
            METADATA_START_COL = 12  # Index 0-based: colonnes 12-23 sont les métadonnées
            NUM_METADATA_COLS = 12   # 4 sites x 3 IDs
            
            for sheet_name, rows in sheets_data.items():
                if not rows:
                    continue
                
                worksheet = self.get_or_create_worksheet(sheet_name)
                if not worksheet:
                    continue
                
                expected_header = list(rows[0].keys())
                header_was_empty = sheet_name not in self.headers_initialized
                self._ensure_header(worksheet, sheet_name, expected_header)
                
                data_to_append = [list(row.values()) for row in rows]
                
                print(f"         📋 '{sheet_name}' → {len(data_to_append)} ligne(s)")
                
                try:
                    def append_data():
                        return worksheet.append_rows(
                            data_to_append,
                            value_input_option='RAW',
                            insert_data_option='INSERT_ROWS',
                            table_range=None
                        )
                    
                    # RETRY RÉDUIT + TIMEOUT
                    self._execute_with_retry(append_data, max_retries=2)
                    total_rows_sent += len(data_to_append)
                    feuilles_traitees += 1
                    
                    # Masquer les colonnes métadonnées si c'est une nouvelle feuille
                    # et si elle contient les colonnes métadonnées
                    if header_was_empty and len(expected_header) > METADATA_START_COL:
                        # Vérifier si les colonnes métadonnées existent
                        if any(col.startswith("MatchId_") for col in expected_header):
                            try:
                                self.hide_metadata_columns(worksheet, METADATA_START_COL, NUM_METADATA_COLS)
                            except Exception as e:
                                print(f"         ⚠️  Erreur masquage colonnes: {e}")
                    
                    # PAUSE ADAPTATIVE
                    if feuilles_traitees % 5 == 0:
                        print(f"         ⏸️  Pause quota ({feuilles_traitees}/32 feuilles)...")
                        await asyncio.sleep(15)  # 15s tous les 5 feuilles
                    else:
                        await asyncio.sleep(2)  # 2s entre chaque
                    
                except Exception as e:
                    print(f"         ❌ '{sheet_name}': {e}")
                    await asyncio.sleep(5)
                    continue
            
            if total_rows_sent > 0:
                print(f"      ✅ {total_rows_sent} lignes envoyées ({feuilles_traitees} feuilles)")
                return True
            else:
                print(f"      ⚠️  Aucune donnée envoyée")
                return False
            
        except Exception as e:
            print(f"   ❌ Erreur envoi batch: {e}")
            traceback.print_exc()
            return False

    
    def update_summary(self, force=False):
        """Mettre à jour summary (avec throttle)"""
        
        now = time.time()
        
        if not force:
            if now - self._last_summary_update < 300:
                return
        
        try:
            worksheet = self.get_or_create_worksheet("Summary")
            
            summary_data = [
                ["ALL MATCHES CUMULATIVE - LIVE"],
                ["Dernière mise à jour", now_mauritius_str()],
                ["Fuseau horaire", "Indian/Mauritius (UTC+4)"],
                ["Utilisateur", "antema102"],
                ["Stratégie", "Capture dynamique + externalId + append() + résultats auto"],
                ["Sites", "StevenHills, SuperScore, ToteLePEP, PlayOnline"],
                [""],
                ["⚠️  IMPORTANT : Résultats des matchs"],
                [""],
                ["✅ Matchs APRÈS déploiement v2.5 : Résultats automatiques (3h après fin)"],
                ["❌ Matchs AVANT cette date : Cotes uniquement (pas de résultats)"],
                [""],
                ["Raison : Métadonnées (MatchId, CompId) ajoutées à partir de cette version"],
                [""],
                ["Feuilles disponibles:"],
            ]
            
            for sheet in self.spreadsheet.worksheets():
                if sheet.title != "Summary":
                    summary_data.append([f"  - {sheet.title}"])
            
            def update():
                worksheet.clear()
                return worksheet.update(values=summary_data, range_name='A1')
            
            self._execute_with_retry(update)
            
            self._last_summary_update = now
            
        except Exception as e:
            print(f"   ⚠️ Erreur update summary: {e}")
    
    def hide_metadata_columns(self, worksheet: gspread.Worksheet, start_col_index: int, num_cols: int = 12):
        """
        Cacher les colonnes métadonnées dans Google Sheets.
        
        Args:
            worksheet: Feuille de calcul Google Sheets
            start_col_index: Index de la première colonne à cacher (0-indexed)
            num_cols: Nombre de colonnes à cacher (12 par défaut: 4 sites x 3 IDs)
        """
        try:
            # Utiliser l'API Google Sheets pour masquer les colonnes
            sheet_id = worksheet.id
            
            requests = [{
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_col_index,
                        "endIndex": start_col_index + num_cols
                    },
                    "properties": {
                        "hiddenByUser": True
                    },
                    "fields": "hiddenByUser"
                }
            }]
            
            def batch_update():
                return self.spreadsheet.batch_update({"requests": requests})
            
            self._execute_with_retry(batch_update)
            print(f"         🙈 {num_cols} colonnes métadonnées cachées")
            
        except Exception as e:
            print(f"         ⚠️  Erreur masquage colonnes: {e}")
    
    def update_cell_by_external_id(self, worksheet: gspread.Worksheet, external_id: int, 
                                    col_name: str, value: str) -> bool:
        """
        Mettre à jour une cellule en recherchant par External ID.
        
        Args:
            worksheet: Feuille de calcul
            external_id: ID externe du match
            col_name: Nom de la colonne à mettre à jour
            value: Valeur à écrire
            
        Returns:
            True si mise à jour réussie, False sinon
        """
        try:
            # Récupérer toutes les valeurs
            def get_all():
                return worksheet.get_all_values()
            
            all_values = self._execute_with_retry(get_all)
            
            if not all_values:
                return False
            
            header = all_values[0]
            
            # Trouver les indices des colonnes
            try:
                external_id_col = header.index("External ID")
                target_col = header.index(col_name)
            except ValueError:
                return False
            
            # Trouver la ligne avec cet external_id
            for row_idx, row in enumerate(all_values[1:], start=2):
                if len(row) > external_id_col:
                    try:
                        if int(row[external_id_col]) == external_id:
                            # Mettre à jour la cellule
                            cell_address = gspread.utils.rowcol_to_a1(row_idx, target_col + 1)
                            
                            def update_cell():
                                return worksheet.update(values=[[value]], range_name=cell_address)
                            
                            self._execute_with_retry(update_cell)
                            return True
                    except (ValueError, TypeError):
                        continue
            
            return False
            
        except Exception as e:
            print(f"         ⚠️  Erreur update cell: {e}")
            return False
    
    def batch_update_cells(self, worksheet: gspread.Worksheet, updates: List[Tuple[int, int, str]]):
        """
        Mettre à jour plusieurs cellules en batch.
        
        Args:
            worksheet: Feuille de calcul
            updates: Liste de tuples (row, col, value) - row et col sont 1-indexed
        """
        try:
            if not updates:
                return
            
            # Préparer les données pour batch_update
            cell_updates = []
            for row, col, value in updates:
                cell_address = gspread.utils.rowcol_to_a1(row, col)
                cell_updates.append({
                    "range": cell_address,
                    "values": [[value]]
                })
            
            def batch_update():
                return worksheet.batch_update(cell_updates)
            
            self._execute_with_retry(batch_update)
            
        except Exception as e:
            print(f"         ⚠️  Erreur batch update cells: {e}")


class MultiSitesOddsTrackerFinal:
    """Tracker FINAL avec externalId + Capture dynamique + append + fix boucle infinie"""
    
    def __init__(self, output_dir="multi_sites_odds"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        (self.output_dir / "comparison").mkdir(exist_ok=True)
        
        self.session: Optional[aiohttp.ClientSession] = None
        
        self.matches_by_external_id: Dict[int, Dict[str, dict]] = defaultdict(dict)
        self.captured_odds: Dict[int, Dict[str, dict]] = defaultdict(dict)
        self.closed_sites: Dict[int, Set[str]] = defaultdict(set)
        self.completed_matches: Set[int] = set()
        self.matches_info_archive: Dict[int, Dict[str, dict]] = {}
        
        self.matches_snapshot: Dict[int, Dict[str, bool]] = {}
        self.early_closed: Dict[int, Set[str]] = defaultdict(set)
        self.current_date = now_mauritius_date_str()
        self.last_getsport_full = None
        
        self.finalization_queue = MatchFinalizationQueue(
            batch_size=20,
            min_interval_seconds=10
        )
        
        self.iteration = 0
        self.start_time = now_mauritius()
        
        self.gsheets = GoogleSheetsManager(
            GOOGLE_SHEETS_CONFIG["credentials_file"],
            GOOGLE_SHEETS_CONFIG["sheet_name"]
        )
        
        self.local_cumulative_excel = self.output_dir / "ALL_MATCHES_CUMULATIVE_LIVE.xlsx"

        self.api_health = APIHealthMonitor()
        
        # Système de retry pour matchs sans cotes
        self.matches_without_odds_retry: Dict[int, Dict[str, any]] = {}
        self.max_retry_attempts = 5  # Nombre max de tentatives

        # Persistence état/cache
        self.state_file = self.output_dir / "tracker_state.pkl"
        self._last_cache_save = 0.0
        self.cache_save_min_interval = 30  # secondes (anti-spam disque)

        # Anti double-finalisation
        self.finalizing_in_progress: Set[int] = set()
        
        # ===== NOUVEAUX: Système de résultats automatiques =====
        # Queue pour récupération des résultats (avec délai 3h)
        self.results_queue = MatchResultsQueue(
            delay_hours=RESULTS_DELAY_HOURS,
            max_retries=MAX_RESULTS_RETRY
        )
        
        # Set des matchs dont on a déjà récupéré les résultats
        self.matches_with_results: Set[int] = set()
        
        # Archive des métadonnées pour récupération résultats
        # Structure: {external_id: {site_key: {matchId, competitionId, sportId, ...}}}
        self.matches_info_archive_for_results: Dict[int, Dict[str, dict]] = {}
        
        # Cache des feuilles avec colonnes métadonnées masquées
        self.sheets_with_hidden_cols: Set[str] = set()
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        
        if not self.gsheets.connect():
            raise Exception("Impossible de se connecter à Google Sheets")
        
        # Charger l'état persistant (si présent)
        self.load_cache_from_disk()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sauvegarder à la fermeture
        self.save_cache_to_disk(force=True)
        if self.session:
            await self.session.close()

    # ----- Persistence helpers -----
    def save_cache_to_disk(self, force: bool = False):
        """Sauvegarde l'état RAM sur disque (pickle) avec anti-spam."""
        try:
            now = time.time()
            if not force and (now - self._last_cache_save) < self.cache_save_min_interval:
                return
            data = {
                "captured_odds": dict(self.captured_odds),
                "matches_info_archive": self.matches_info_archive,
                "matches_by_external_id": dict(self.matches_by_external_id),
                "closed_sites": dict(self.closed_sites),
                "completed_matches": list(self.completed_matches),
                "early_closed": dict(self.early_closed),
                "matches_snapshot": self.matches_snapshot,
                "matches_without_odds_retry": self.matches_without_odds_retry,
                "current_date": self.current_date,
                "iteration": self.iteration,
                "finalizing_in_progress": list(self.finalizing_in_progress),
                # Nouveaux champs pour résultats automatiques
                "results_queue": self.results_queue.to_serializable(),
                "matches_with_results": list(self.matches_with_results),
                "matches_info_archive_for_results": self.matches_info_archive_for_results,
                "sheets_with_hidden_cols": list(self.sheets_with_hidden_cols),
                "saved_at": now_mauritius_str(),
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(data, f)
            self._last_cache_save = now
            print(f"💾 État sauvegardé ({self.state_file.name})")
        except Exception as e:
            print(f"❌ Erreur sauvegarde cache : {e}")

    def _to_defaultdict(self, factory, d: Optional[dict]):
        """Transformer un dict en defaultdict avec factory pour compatibilité."""
        return defaultdict(factory, d or {})

    def load_cache_from_disk(self):
        """Charge l'état disque en RAM (pickle) et restaure les defaultdict."""
        try:
            if not self.state_file.exists():
                print("ℹ️ Aucun cache sur disque à charger.")
                return
            with open(self.state_file, 'rb') as f:
                data = pickle.load(f)

            # Restaurer en conservant les types attendus
            self.captured_odds = self._to_defaultdict(dict, data.get("captured_odds"))
            self.matches_by_external_id = self._to_defaultdict(dict, data.get("matches_by_external_id"))
            # closed_sites et early_closed: sets par valeur
            loaded_closed = data.get("closed_sites") or {}
            loaded_early = data.get("early_closed") or {}
            self.closed_sites = defaultdict(set, {int(k): set(v) for k, v in loaded_closed.items()})
            self.early_closed = defaultdict(set, {int(k): set(v) for k, v in loaded_early.items()})

            self.matches_info_archive = data.get("matches_info_archive") or {}
            self.completed_matches = set(data.get("completed_matches") or [])
            self.matches_snapshot = data.get("matches_snapshot") or {}
            self.matches_without_odds_retry = data.get("matches_without_odds_retry") or {}
            self.current_date = data.get("current_date", self.current_date)
            self.iteration = int(data.get("iteration", self.iteration))
            self.finalizing_in_progress = set(data.get("finalizing_in_progress") or [])
            
            # Restaurer champs résultats automatiques
            results_queue_data = data.get("results_queue") or []
            self.results_queue.from_serializable(results_queue_data)
            self.matches_with_results = set(data.get("matches_with_results") or [])
            self.matches_info_archive_for_results = data.get("matches_info_archive_for_results") or {}
            self.sheets_with_hidden_cols = set(data.get("sheets_with_hidden_cols") or [])

            # Log résumé
            total_sites_cached = sum(len(s) for s in self.captured_odds.values())
            results_pending = len(self.results_queue)
            print(f"✅ Cache rechargé ({self.state_file.name}) — cotes en cache: {total_sites_cached} sites, matchs suivis: {len(self.matches_info_archive)}, résultats en attente: {results_pending}")
        except Exception as e:
            print(f"❌ Erreur chargement cache : {e}")
    
    # ----- Utils & odds handling -----
    def safe_float(self, value, default=0.0) -> float:
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                cleaned = value.strip().replace(',', '.')
                return float(cleaned) if cleaned else default
            return default
        except:
            return default
    
    def _get_time_until_match(self, start_time_str: str) -> Optional[float]:
        """Retourner le temps restant avant le match (en minutes)"""
        try:
            match_time_str = start_time_str.replace(',', '').strip()
            current_year = now_mauritius().year
            if str(current_year) not in match_time_str:
                match_time_str = f"{match_time_str} {current_year}"
            
            match_time = date_parser.parse(match_time_str)
            if match_time.tzinfo is None:
                match_time = match_time.replace(tzinfo=MAURITIUS_TZ)
            
            now = now_mauritius()
            time_diff_minutes = (match_time - now).total_seconds() / 60
            return time_diff_minutes
        except:
            return None
    
    def _is_match_close_to_start(self, start_time_str: str, minutes_before: int = 10) -> bool:
        """Vérifier si le match commence bientôt"""
        try:
            time_diff_minutes = self._get_time_until_match(start_time_str)
            
            if time_diff_minutes is None:
                return True
            
            return time_diff_minutes <= minutes_before
            
        except Exception as e:
            return True
    
    def _odds_have_changed(self, old_odds: dict, new_odds: dict) -> bool:
        """Détecter si les cotes ont changé"""
        try:
            old_markets = old_odds.get("markets", {})
            new_markets = new_odds.get("markets", {})
            
            # Vérifier même nombre de marchés
            if len(old_markets) != len(new_markets):
                return True
            
            # Comparer chaque marché
            for market_key in old_markets.keys():
                if market_key not in new_markets:
                    return True
                
                old_market_odds = old_markets[market_key].get("odds", {})
                new_market_odds = new_markets[market_key].get("odds", {})
                
                # Comparer les valeurs
                for option_key in old_market_odds.keys():
                    if option_key not in new_market_odds:
                        return True
                    
                    old_value = old_market_odds[option_key].get("odd", 0)
                    new_value = new_market_odds[option_key].get("odd", 0)
                    
                    # Différence > 0.01
                    if abs(old_value - new_value) > 0.01:
                        return True
            
            return False
        except:
            return True  # En cas d'erreur, considérer comme changé
        

    async def fetch(self, url: str, params: dict = None, site_name: str = "") -> Optional[dict]:
        """Fetch avec détection d'erreurs détaillée"""
        last_error = None
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    
                    # Erreurs récupérables
                    if resp.status in (429, 500, 502, 503, 504):
                        last_error = f"HTTP {resp.status}"
                        if attempt < MAX_RETRIES:
                            wait = min(2 ** attempt, 5)
                            await asyncio.sleep(wait)
                            continue
                        else:
                            # Échec après toutes les tentatives
                            return None
                    else:
                        # Erreur non récupérable (404, 403, etc.)
                        last_error = f"HTTP {resp.status}"
                        return None
            
            except asyncio.TimeoutError:
                last_error = f"Timeout ({TIMEOUT}s)"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
            
            except aiohttp.ClientError as e:
                last_error = f"ClientError: {type(e).__name__}"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
            
            except Exception as e:
                last_error = f"{type(e).__name__}"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
        
        # Si on arrive ici, toutes les tentatives ont échoué
        return None
    
    async def fetch_post(self, url: str, data: dict = None, site_name: str = "") -> Optional[dict]:
        """
        Fetch POST avec détection d'erreurs détaillée.
        
        Args:
            url: URL à appeler
            data: Données form-encoded à envoyer
            site_name: Nom du site pour les logs
            
        Returns:
            JSON de la réponse ou None si échec
        """
        last_error = None
        headers = HEADERS.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.session.post(url, headers=headers, data=data, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    
                    if resp.status in (429, 500, 502, 503, 504):
                        last_error = f"HTTP {resp.status}"
                        if attempt < MAX_RETRIES:
                            wait = min(2 ** attempt, 5)
                            await asyncio.sleep(wait)
                            continue
                        else:
                            return None
                    else:
                        last_error = f"HTTP {resp.status}"
                        return None
            
            except asyncio.TimeoutError:
                last_error = f"Timeout ({TIMEOUT}s)"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
            
            except aiohttp.ClientError as e:
                last_error = f"ClientError: {type(e).__name__}"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
            
            except Exception as e:
                last_error = f"{type(e).__name__}"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None
        
        return None
    
    async def fetch_searchresult(self, site_key: str, date: str) -> Optional[dict]:
        """
        Appeler POST /webapi/searchresult pour récupérer la liste des matchs terminés.
        
        Args:
            site_key: Clé du site (stevenhills, superscore, etc.)
            date: Date au format DD/MM/YYYY
            
        Returns:
            JSON de la réponse ou None si échec
        """
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/searchresult"
        
        data = {"DateFrom": date}
        
        return await self.fetch_post(url, data, SITES[site_key]["name"])
    
    async def fetch_getmatchdata(self, site_key: str, match_id: int, sport_id: int, 
                                  competition_id: int) -> Optional[dict]:
        """
        Appeler POST /WebApi/GetMatchData pour récupérer les résultats détaillés par marché.
        
        Args:
            site_key: Clé du site
            match_id: ID du match sur ce site
            sport_id: ID du sport (1 pour soccer)
            competition_id: ID de la compétition sur ce site
            
        Returns:
            JSON de la réponse ou None si échec
        """
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/WebApi/GetMatchData"
        
        data = {
            "matchId": match_id,
            "sportId": sport_id,
            "competitionId": competition_id
        }
        
        return await self.fetch_post(url, data, SITES[site_key]["name"])
    
    def _find_match_in_searchresult(self, search_result: dict, match_id: int) -> Optional[dict]:
        """
        Trouver un match dans la réponse searchresult.
        
        Args:
            search_result: Réponse JSON de searchresult
            match_id: ID du match à chercher
            
        Returns:
            Données du match ou None si non trouvé
        """
        if not search_result or not search_result.get("isSuccess"):
            return None
        
        transactions = search_result.get("transaction", [])
        
        for country_data in transactions:
            matches = country_data.get("matches", [])
            for match in matches:
                if match.get("matchId") == match_id:
                    return match
        
        return None
    
    def _parse_market_results(self, market_data: dict) -> dict:
        """
        Parser les résultats de GetMatchData.
        
        Args:
            market_data: Réponse JSON de GetMatchData
            
        Returns:
            Dict avec les résultats par marché {market_key: result}
        """
        results = {}
        
        if not market_data or not market_data.get("isSuccess"):
            return results
        
        transactions = market_data.get("transaction", [])
        
        for market in transactions:
            market_code = market.get("marketCode", "")
            period_code = market.get("periodCode", "")
            line = market.get("line", "")
            full_time_result = market.get("fullTime", "")
            
            # Construire la clé du marché
            if line:
                market_key = f"{market_code}_{period_code}_{line}"
            else:
                market_key = f"{market_code}_{period_code}"
            
            results[market_key] = {
                "result": full_time_result,
                "display_name": market.get("marketDisplayName", ""),
                "market_code": market_code,
                "period_code": period_code,
                "line": line
            }
        
        return results
    
    def _get_consensus_scores(self, scores_by_site: Dict[str, dict]) -> dict:
        """
        Déterminer le consensus des scores (majorité 2/4 ou 3/4).
        
        Args:
            scores_by_site: Dict {site_key: {fullTime, halfTime, secondHalfTime}}
            
        Returns:
            Dict avec les scores consensus {fullTime, halfTime, secondHalfTime}
        """
        if not scores_by_site:
            return {}
        
        # Collecter les scores par type
        full_times = []
        half_times = []
        second_half_times = []
        
        for site_key, scores in scores_by_site.items():
            if scores.get("fullTime"):
                full_times.append(scores["fullTime"])
            if scores.get("halfTime"):
                half_times.append(scores["halfTime"])
            if scores.get("secondHalfTime"):
                second_half_times.append(scores["secondHalfTime"])
        
        def get_majority(values: list) -> Optional[str]:
            """Retourner la valeur majoritaire ou la première si égalité."""
            if not values:
                return None
            
            from collections import Counter
            counter = Counter(values)
            most_common = counter.most_common(1)
            
            if most_common:
                return most_common[0][0]
            return values[0] if values else None
        
        return {
            "fullTime": get_majority(full_times),
            "halfTime": get_majority(half_times),
            "secondHalfTime": get_majority(second_half_times)
        }
    
    def _get_consensus_market_results(self, results_by_site: Dict[str, dict]) -> dict:
        """
        Déterminer le consensus des résultats par marché.
        
        Args:
            results_by_site: Dict {site_key: {market_key: {result, ...}}}
            
        Returns:
            Dict avec les résultats consensus {market_key: result}
        """
        if not results_by_site:
            return {}
        
        # Collecter tous les marchés uniques
        all_market_keys = set()
        for site_results in results_by_site.values():
            all_market_keys.update(site_results.keys())
        
        consensus = {}
        
        for market_key in all_market_keys:
            results = []
            for site_key, site_results in results_by_site.items():
                if market_key in site_results and site_results[market_key].get("result"):
                    results.append(site_results[market_key]["result"])
            
            if results:
                from collections import Counter
                counter = Counter(results)
                most_common = counter.most_common(1)
                consensus[market_key] = most_common[0][0] if most_common else results[0]
        
        return consensus
    
    async def fetch_results_for_match(self, external_id: int) -> Optional[dict]:
        """
        Récupérer les résultats pour UN match (4 sites en parallèle).
        
        Args:
            external_id: ID externe du match
            
        Returns:
            Dict avec scores et résultats par marché, ou None si non disponible
        """
        # Récupérer les métadonnées du match
        match_metadata = self.matches_info_archive_for_results.get(external_id)
        
        if not match_metadata:
            print(f"      ⚠️  Match {external_id}: Pas de métadonnées pour récupération résultats")
            return None
        
        # Date au format DD/MM/YYYY pour searchresult
        # Utiliser la date du match depuis les métadonnées
        first_site_data = list(match_metadata.values())[0]
        match_start_time = first_site_data.get("start_time", "")
        
        try:
            # Parser la date du match
            match_dt = date_parser.parse(match_start_time.replace(',', '').strip())
            search_date = match_dt.strftime("%d/%m/%Y")
        except:
            search_date = now_mauritius().strftime("%d/%m/%Y")
        
        # ===== ÉTAPE 1: Appeler searchresult pour les 4 sites en parallèle =====
        search_tasks = []
        sites_with_metadata = list(match_metadata.keys())
        
        for site_key in sites_with_metadata:
            search_tasks.append(self.fetch_searchresult(site_key, search_date))
        
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
        
        # Collecter les scores par site
        scores_by_site = {}
        site_match_ids = {}  # Pour GetMatchData
        
        for site_key, result in zip(sites_with_metadata, search_results):
            if isinstance(result, Exception) or not result:
                continue
            
            site_meta = match_metadata[site_key]
            match_id = site_meta.get("match_id")
            
            match_info = self._find_match_in_searchresult(result, match_id)
            
            if match_info:
                scores_by_site[site_key] = {
                    "fullTime": match_info.get("fullTime", ""),
                    "halfTime": match_info.get("halfTime", ""),
                    "secondHalfTime": match_info.get("secondHalfTime", "")
                }
                site_match_ids[site_key] = {
                    "match_id": match_id,
                    "sport_id": match_info.get("sportId", 1),
                    "competition_id": match_info.get("competitionId")
                }
        
        if not scores_by_site:
            print(f"      ⚠️  Match {external_id}: Aucun score trouvé sur les 4 sites")
            return None
        
        # Consensus des scores
        consensus_scores = self._get_consensus_scores(scores_by_site)
        
        print(f"      📊 Match {external_id}: Scores trouvés sur {len(scores_by_site)} site(s)")
        print(f"         FT: {consensus_scores.get('fullTime', 'N/A')}, HT: {consensus_scores.get('halfTime', 'N/A')}, 2H: {consensus_scores.get('secondHalfTime', 'N/A')}")
        
        # ===== ÉTAPE 2: Appeler GetMatchData pour les résultats par marché =====
        market_tasks = []
        sites_for_market = []
        
        for site_key, ids in site_match_ids.items():
            task = self.fetch_getmatchdata(
                site_key,
                ids["match_id"],
                ids["sport_id"],
                ids["competition_id"]
            )
            market_tasks.append(task)
            sites_for_market.append(site_key)
        
        market_results = await asyncio.gather(*market_tasks, return_exceptions=True)
        
        # Collecter les résultats par marché par site
        results_by_site = {}
        
        for site_key, result in zip(sites_for_market, market_results):
            if isinstance(result, Exception) or not result:
                continue
            
            parsed = self._parse_market_results(result)
            if parsed:
                results_by_site[site_key] = parsed
        
        # Consensus des résultats par marché
        consensus_market_results = self._get_consensus_market_results(results_by_site)
        
        print(f"      📋 Match {external_id}: {len(consensus_market_results)} marchés avec résultats")
        
        return {
            "external_id": external_id,
            "scores": consensus_scores,
            "market_results": consensus_market_results,
            "sites_responded": len(scores_by_site)
        }
    
    async def update_results_in_sheets(self, external_id: int, results: dict):
        """
        Mettre à jour les résultats dans Google Sheets.
        
        Args:
            external_id: ID externe du match
            results: Dict avec scores et market_results
        """
        scores = results.get("scores", {})
        market_results = results.get("market_results", {})
        
        if not scores and not market_results:
            return
        
        print(f"      📝 Mise à jour Google Sheets pour match {external_id}...")
        
        sheets_updated = 0
        
        # Pour chaque feuille (marché)
        for market_key, sheet_name in MARKET_SHEET_MAPPING.items():
            try:
                worksheet = self.gsheets.get_or_create_worksheet(sheet_name)
                if not worksheet:
                    continue
                
                # Déterminer quel score afficher selon le marché
                score_type = SCORE_MAPPING.get(market_key, "FT")
                
                if score_type == "FT":
                    score_value = scores.get("fullTime", "")
                elif score_type == "HT":
                    score_value = scores.get("halfTime", "")
                elif score_type == "2H":
                    score_value = scores.get("secondHalfTime", "")
                else:
                    score_value = scores.get("fullTime", "")
                
                # Résultat du marché
                result_value = market_results.get(market_key, "")
                if isinstance(result_value, dict):
                    result_value = result_value.get("result", "")
                
                if not score_value and not result_value:
                    continue
                
                # Mettre à jour les colonnes Score et Résultat
                success_score = False
                success_result = False
                
                if score_value:
                    success_score = self.gsheets.update_cell_by_external_id(
                        worksheet, external_id, "Score", score_value
                    )
                
                if result_value:
                    success_result = self.gsheets.update_cell_by_external_id(
                        worksheet, external_id, "Résultat", result_value
                    )
                
                if success_score or success_result:
                    sheets_updated += 1
                
                # Pause anti-quota
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"         ⚠️  Erreur mise à jour {sheet_name}: {e}")
                continue
        
        print(f"      ✅ {sheets_updated} feuilles mises à jour")
    
    def update_results_in_excel(self, external_id: int, results: dict):
        """
        Mettre à jour les résultats dans Excel local.
        
        Args:
            external_id: ID externe du match
            results: Dict avec scores et market_results
        """
        scores = results.get("scores", {})
        market_results = results.get("market_results", {})
        
        if not scores and not market_results:
            return
        
        if not self.local_cumulative_excel.exists():
            return
        
        try:
            # Lire le fichier Excel
            excel_sheets = pd.read_excel(
                self.local_cumulative_excel, 
                sheet_name=None, 
                engine='openpyxl'
            )
            
            sheets_modified = False
            
            for sheet_name, df in excel_sheets.items():
                if sheet_name == "Summary":
                    continue
                
                if "External ID" not in df.columns:
                    continue
                
                # Trouver la ligne avec cet external_id
                mask = df["External ID"] == external_id
                
                if not mask.any():
                    continue
                
                # Trouver le market_key correspondant à ce sheet_name
                market_key = None
                for mk, sn in MARKET_SHEET_MAPPING.items():
                    if sn == sheet_name:
                        market_key = mk
                        break
                
                if not market_key:
                    # Essayer avec le nom de feuille directement
                    market_key = sheet_name
                
                # Déterminer le score approprié
                score_type = SCORE_MAPPING.get(market_key, "FT")
                
                if score_type == "FT":
                    score_value = scores.get("fullTime", "")
                elif score_type == "HT":
                    score_value = scores.get("halfTime", "")
                elif score_type == "2H":
                    score_value = scores.get("secondHalfTime", "")
                else:
                    score_value = scores.get("fullTime", "")
                
                # Résultat du marché
                result_value = market_results.get(market_key, "")
                if isinstance(result_value, dict):
                    result_value = result_value.get("result", "")
                
                # Mettre à jour les colonnes si elles existent
                if "Score" in df.columns and score_value:
                    df.loc[mask, "Score"] = score_value
                    sheets_modified = True
                
                if "Résultat" in df.columns and result_value:
                    df.loc[mask, "Résultat"] = result_value
                    sheets_modified = True
                
                excel_sheets[sheet_name] = df
            
            # Réécrire le fichier Excel si modifié
            if sheets_modified:
                with pd.ExcelWriter(self.local_cumulative_excel, engine='openpyxl', mode='w') as writer:
                    for sheet_name, df in excel_sheets.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"      ✅ Excel local mis à jour pour match {external_id}")
            
        except Exception as e:
            print(f"      ⚠️  Erreur mise à jour Excel: {e}")
    
    async def check_and_update_results(self):
        """
        Vérifier et mettre à jour les résultats (toutes les heures).
        Cette fonction est appelée périodiquement pour récupérer les résultats
        des matchs terminés depuis plus de 3 heures.
        """
        ready_matches = self.results_queue.get_ready_matches()
        
        if not ready_matches:
            print(f"\n   📊 Aucun match prêt pour récupération des résultats")
            return
        
        print(f"\n{'='*70}")
        print(f"📊 RÉCUPÉRATION RÉSULTATS ({len(ready_matches)} matchs prêts)")
        print(f"{'='*70}")
        
        for match_item in ready_matches:
            external_id = match_item["external_id"]
            retry_count = match_item["retry_count"]
            
            print(f"\n   🔍 Match {external_id} (tentative {retry_count + 1}/{MAX_RESULTS_RETRY})...")
            
            try:
                # Récupérer les résultats
                results = await self.fetch_results_for_match(external_id)
                
                if results and results.get("scores"):
                    # Mise à jour Google Sheets
                    await self.update_results_in_sheets(external_id, results)
                    
                    # Mise à jour Excel local
                    self.update_results_in_excel(external_id, results)
                    
                    # Marquer comme terminé
                    self.results_queue.mark_completed(external_id)
                    self.matches_with_results.add(external_id)
                    
                    print(f"   ✅ Match {external_id}: Résultats récupérés et mis à jour")
                    
                else:
                    # Incrémenter retry
                    can_retry = self.results_queue.increment_retry(external_id)
                    
                    if can_retry:
                        print(f"   ⏳ Match {external_id}: Résultats non disponibles → retry dans 1h")
                    else:
                        print(f"   ❌ Match {external_id}: Max tentatives atteint → abandonné")
                        self.results_queue.mark_failed(external_id)
                
                # Pause entre chaque match
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"   ❌ Erreur match {external_id}: {e}")
                self.results_queue.increment_retry(external_id)
        
        # Sauvegarder l'état
        self.save_cache_to_disk(force=True)
        
        print(f"\n   📊 Résultats en attente: {len(self.results_queue)} matchs")
    
    
    def _parse_getsport_match_data(self, match_str: str, competitions: dict) -> Optional[dict]:
        """Parser avec externalId (index 28)"""
        try:
            parts = match_str.split(";")
            
            if len(parts) < 29:
                return None
            
            match_id = int(parts[0])
            competition_id = int(parts[1])
            
            external_id_str = parts[28]
            
            if not external_id_str or not external_id_str.isdigit():
                return None
            
            external_id = int(external_id_str)
            
            if external_id == 0:
                return None
            
            return {
                "match_id": match_id,
                "competition_id": competition_id,
                "external_id": external_id,
                "match_name": parts[2],
                "start_time": parts[3],
                "competition_name": competitions.get(competition_id, {}).get("name", ""),
                "category": competitions.get(competition_id, {}).get("country", ""),
                "market_count": int(parts[14]) if len(parts) > 14 and parts[14].isdigit() else 0,
                "sport_id": "soccer",
            }
        except:
            return None
    
    async def get_sport_page(self, site_key: str, date: str, page_no: int, inclusive: int = 0) -> Optional[dict]:
        """Récupérer une page via GetSport"""
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/GetSport"
        params = {
            "sportId": "soccer",
            "date": f"{date}T00:00:00+04:00",
            "category": "",
            "competitionId": 0,
            "pageNo": page_no,
            "inclusive": inclusive,
            "matchid": 0,
            "periodCode": "today"
        }
        return await self.fetch(url, params, SITES[site_key]["name"])
    
    async def get_all_matches_for_site_date(self, site_key: str, date: str) -> Dict[int, dict]:
        """Récupérer TOUS les matchs d'un site pour une date avec pagination parallèle"""
        
        page1_data = await self.get_sport_page(site_key, date, 1, inclusive=1)
        
        if not page1_data:
            return {}
        
        total_pages = page1_data.get("totalPages", 1)
        
        competitions = {}
        comp_data = page1_data.get("competitionData", "")
        if comp_data:
            for comp_str in comp_data.split("|"):
                if not comp_str.strip():
                    continue
                parts = comp_str.split(";")
                if len(parts) >= 6:
                    comp_id = int(parts[0])
                    competitions[comp_id] = {
                        "id": comp_id,
                        "name": parts[1],
                        "country": parts[5]
                    }
        
        all_matches = {}
        match_data = page1_data.get("matchData", "")
        if match_data:
            for match_str in match_data.split("|"):
                if not match_str.strip():
                    continue
                match_info = self._parse_getsport_match_data(match_str, competitions)
                if match_info:
                    match_info["site"] = site_key
                    all_matches[match_info["external_id"]] = match_info
        
        if total_pages > 1:
            tasks = [
                self.get_sport_page(site_key, date, page_no, inclusive=0)
                for page_no in range(2, total_pages + 1)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception) or not result:
                    continue
                
                match_data = result.get("matchData", "")
                if not match_data:
                    continue
                
                for match_str in match_data.split("|"):
                    if not match_str.strip():
                        continue
                    match_info = self._parse_getsport_match_data(match_str, competitions)
                    if match_info:
                        match_info["site"] = site_key
                        all_matches[match_info["external_id"]] = match_info
        
        return all_matches
    
    
    async def get_all_sites_matches(self, date: str) -> Dict[str, Dict[int, dict]]:
        """Récupérer TOUS les matchs des 4 sites avec monitoring"""
        
        tasks = [
            self.get_all_matches_for_site_date(site_key, date)
            for site_key in SITES.keys()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        matches_by_site = {}
        total_errors = 0
        
        for site_key, result in zip(SITES.keys(), results):
            if isinstance(result, Exception):
                matches_by_site[site_key] = {}
                error_msg = f"{type(result).__name__}"
                print(f"   ❌ {SITES[site_key]['name']:15s}: {error_msg}")
                self.api_health.record_getsport(site_key, False, error_msg)
            
            elif result is None or len(result) == 0:
                matches_by_site[site_key] = {}
                error_msg = "Aucun match retourné"
                print(f"   ⚠️  {SITES[site_key]['name']:15s}: {error_msg}")
                self.api_health.record_getsport(site_key, False, error_msg)
            
            else:
                matches_by_site[site_key] = result
                print(f"   ✅ {SITES[site_key]['name']:15s}: {len(result):3d} matchs")
                self.api_health.record_getsport(site_key, True)
        
        return matches_by_site
    
    
    async def get_match_details(self, site_key: str, sport_id: str, competition_id: int, match_id: int):
        """Récupérer les détails d'un match via GetMatch"""
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/GetMatch"
        params = {
            "sportId": sport_id.lower(),
            "competitionId": competition_id,
            "matchId": match_id,
            "periodCode": "all"
        }
        return await self.fetch(url, params, SITES[site_key]["name"])
    
    def extract_odds(self, data: dict, site_key: str) -> dict:
        """Extraire les cotes d'une réponse GetMatch"""
        odds = {"site": site_key, "timestamp": now_mauritius_iso(), "markets": {}}
        try:
            competitions = data.get("competitions", [])
            if not competitions:
                return odds
            matches = competitions[0].get("matches", [])
            if not matches:
                return odds
            match_data = matches[0]
            markets = match_data.get("markets", [])
            
            competition_name = competitions[0].get("name", "")
            if competition_name:
                odds["competition_name"] = competition_name
            
            for market in markets:
                market_code = market.get("marketCode", "")
                market_period = market.get("periodCode", "")
                market_line = market.get("marketLine", "")
                market_display = market.get("marketDisplayName", "")
                key = f"{market_code}_{market_period}_{market_line}".strip("_")
                selections = market.get("selectionList", [])
                market_odds = {}
                
                for sel in selections:
                    option_code = sel.get("optionCode", "")
                    option_name = sel.get("name", "")
                    odd_value = self.safe_float(sel.get("companyOdds"), 0.0)
                    dict_key = option_code if option_code else option_name
                    market_odds[dict_key] = {
                        "name": option_name,
                        "code": option_code,
                        "odd": odd_value,
                        "option_no": sel.get("optionNo", ""),
                    }
                
                odds["markets"][key] = {
                    "display_name": market_display,
                    "period": market_period,
                    "line": market_line,
                    "market_code": market_code,
                    "odds": market_odds,
                }
        except Exception as e:
            pass
        return odds
    
    def _get_close_matches(self, window_minutes: int = 60) -> List[int]:
        """Identifier les matchs proches du début"""
        close_matches = []
        
        all_match_ids = set(self.matches_by_external_id.keys()) | set(self.matches_info_archive.keys())
        
        for external_id in all_match_ids:
            if external_id in self.completed_matches:
                continue
            
            match_info_dict = self.matches_by_external_id.get(external_id) or self.matches_info_archive.get(external_id)
            if not match_info_dict:
                continue
            
            first_match = list(match_info_dict.values())[0]
            start_time = first_match.get("start_time", "")
            
            time_until_match = self._get_time_until_match(start_time)
            
            if time_until_match is None:
                continue
            
            if -5 <= time_until_match <= window_minutes:
                close_matches.append(external_id)
        
        return close_matches
    
    async def verify_close_matches_availability(self):
        """Vérifier la disponibilité des matchs proches avec statistiques"""
        
        close_matches = self._get_close_matches(CLOSE_MATCH_WINDOW_MINUTES)
        
        if not close_matches:
            return
        
        stats = {
            "total_attempts": 0,
            "success": 0,
            "updated": 0,
            "closed": 0,
            "failed": 0,
            "errors_by_site": defaultdict(int)
        }
        
        capture_count = 0
        
        for external_id in close_matches:
            match_info_dict = self.matches_by_external_id.get(external_id) or self.matches_info_archive.get(external_id)
            if not match_info_dict:
                continue
            
            open_sites = set(match_info_dict.keys()) - self.closed_sites[external_id]
            
            if not open_sites:
                continue
            
            for site_key in open_sites:
                match_info = match_info_dict[site_key]
                stats["total_attempts"] += 1
                
                try:
                    data = await self.get_match_details(
                        site_key,
                        match_info["sport_id"],
                        match_info["competition_id"],
                        match_info["match_id"]
                    )
                    
                    if data is None:
                        # Match fermé
                        self.closed_sites[external_id].add(site_key)
                        await self.capture_odds_for_sites(external_id, {site_key}, force_refresh=True)
                        stats["closed"] += 1
                        
                        self.api_health.record_getmatch(site_key, False, "Match fermé")
                    
                    else:
                        odds = self.extract_odds(data, site_key)
                        
                        if odds and odds.get("markets"):
                            if "competition_name" in odds and odds["competition_name"]:
                                match_info["competition_name"] = odds["competition_name"]
                            
                            is_update = external_id in self.captured_odds and site_key in self.captured_odds[external_id]
                            
                            # Détecter changement de cotes
                            if is_update:
                                old_odds = self.captured_odds[external_id][site_key]
                                has_changed = self._odds_have_changed(old_odds, odds)
                                
                                if has_changed:
                                    print(f"      🔄 {SITES[site_key]['name']}: Cotes MODIFIÉES ({len(odds['markets'])} marchés) 📊")
                                else:
                                    print(f"      ✓ {SITES[site_key]['name']}: Cotes identiques ({len(odds['markets'])} marchés)")
                            
                            self.captured_odds[external_id][site_key] = odds
                            
                            if is_update:
                                stats["updated"] += 1
                            else:
                                capture_count += 1
                                stats["success"] += 1
                            
                            # Sauvegarde (throttled)
                            self.save_cache_to_disk()
                            
                            self.api_health.record_getmatch(site_key, True)
                        else:
                            stats["failed"] += 1
                            self.api_health.record_getmatch(site_key, False, "Pas de marchés")
                
                except Exception as e:
                    stats["failed"] += 1
                    stats["errors_by_site"][site_key] += 1
                    error_msg = f"{type(e).__name__}"
                    self.api_health.record_getmatch(site_key, False, error_msg)
        
        if capture_count > 0:
            print(f"   ✅ Capture dynamique : {capture_count} nouveaux sites")
        
        if stats["total_attempts"] > 0:
            print(f"\n   📊 GetMatch: {stats['total_attempts']} tentatives")
            if stats["success"] > 0:
                print(f"      ✅ Nouveaux : {stats['success']}")
            if stats["updated"] > 0:
                print(f"      🔄 Actualisés : {stats['updated']}")
            if stats["closed"] > 0:
                print(f"      🔒 Fermés : {stats['closed']}")
            if stats["failed"] > 0:
                print(f"      ❌ Erreurs : {stats['failed']}")
            
            if stats["errors_by_site"]:
                print(f"      📉 Erreurs par site :")
                for site_key, count in stats["errors_by_site"].items():
                    print(f"         • {SITES[site_key]['name']:15s}: {count} erreur(s)")

    async def detect_early_closures_and_reopenings(self, current_matches_by_site: Dict[str, Dict[int, dict]]):
        """Détecter fermetures ET capturer IMMÉDIATEMENT avant disparition"""
        
        current_snapshot = defaultdict(dict)
        for site_key, matches in current_matches_by_site.items():
            for external_id in matches.keys():
                current_snapshot[external_id][site_key] = True
        
        for external_id in list(self.early_closed.keys()):
            if external_id in self.completed_matches:
                continue
            
            early_closed_sites = self.early_closed[external_id]
            current_sites = set(current_snapshot.get(external_id, {}).keys())
            
            reopened_sites = early_closed_sites & current_sites
            
            if reopened_sites:
                match_info = self.matches_info_archive.get(external_id)
                if match_info:
                    first_match = list(match_info.values())[0]
                    match_name = first_match.get("match_name", "")
                    
                    print(f"\n   🟢 RÉAPPARITION après fermeture précoce")
                    print(f"      📌 Match {external_id} : {match_name[:40]}")
                    print(f"      ✅ Sites : {', '.join([SITES[s]['name'] for s in reopened_sites])}")
                
                self.early_closed[external_id] -= reopened_sites
                
                if not self.early_closed[external_id]:
                    del self.early_closed[external_id]
        
        for external_id in self.matches_snapshot.keys():
            if external_id in self.completed_matches:
                continue
            
            previous_sites = set(self.matches_snapshot[external_id].keys())
            current_sites = set(current_snapshot.get(external_id, {}).keys())
            
            disappeared_sites = previous_sites - current_sites - self.closed_sites[external_id] - self.early_closed.get(external_id, set())
            
            if disappeared_sites:
                match_info = self.matches_info_archive.get(external_id)
                if not match_info:
                    continue
                
                first_match = list(match_info.values())[0]
                match_name = first_match.get("match_name", "")
                start_time = first_match.get("start_time", "")
                time_until_match = self._get_time_until_match(start_time)
                
                if time_until_match is None:
                    continue
                
                print(f"\n   🔴 Fermeture détectée (GetSport)")
                print(f"      📌 Match {external_id} : {match_name[:40]}")
                print(f"      🕐 Temps avant match : {time_until_match:.0f} min")
                print(f"      ❌ Sites : {', '.join([SITES[s]['name'] for s in disappeared_sites])}")
                
                if time_until_match > 60:
                    print(f"      ⚠️  Fermeture PRÉCOCE (> 1h avant)")
                    print(f"      📸 Tentative capture AVANT disparition...")
                    
                    await self.capture_odds_for_sites(external_id, disappeared_sites, force_refresh=True)
                    
                    for site_key in disappeared_sites:
                        self.early_closed[external_id].add(site_key)
                
                else:
                    print(f"      📸 Capture des cotes...")
                    
                    await self.capture_odds_for_sites(external_id, disappeared_sites, force_refresh=True)
                    self.closed_sites[external_id].update(disappeared_sites)
        
        self.matches_snapshot = current_snapshot
        # Sauvegarde (throttled) après mise à jour d'état
        self.save_cache_to_disk()
    
    async def capture_odds_for_sites(self, external_id: int, site_keys: Set[str], force_refresh: bool = False):
        """Capturer les cotes avec mise à jour continue pour matchs proches"""
        tasks = []
        sites_to_capture = []
        
        for site_key in site_keys:
            match_info = None
            if external_id in self.matches_by_external_id and site_key in self.matches_by_external_id[external_id]:
                match_info = self.matches_by_external_id[external_id][site_key]
            elif external_id in self.matches_info_archive and site_key in self.matches_info_archive[external_id]:
                match_info = self.matches_info_archive[external_id][site_key]
            
            if not match_info:
                continue
            
            # VÉRIFIER SI MATCH PROCHE (< 60 min)
            start_time = match_info.get("start_time", "")
            time_until = self._get_time_until_match(start_time)
            
            is_close_match = (time_until is not None and time_until <= 60)
            
            has_cache = external_id in self.captured_odds and site_key in self.captured_odds[external_id]
            
            # DÉCISION : Capturer si...
            should_capture = (
                force_refresh or                    # Force demandé
                not has_cache or                    # Pas de cache
                is_close_match                      # Match proche → UPDATE continu
            )
            
            if not should_capture:
                continue
            
            # LOG différent selon situation
            if has_cache and is_close_match:
                old_timestamp = self.captured_odds[external_id][site_key].get("timestamp", "")
                try:
                    old_dt = date_parser.parse(old_timestamp)
                    age_minutes = (now_mauritius() - old_dt).total_seconds() / 60
                    print(f"         🔄 {SITES[site_key]['name']}: Mise à jour cotes (dernière capture il y a {age_minutes:.0f} min)")
                except:
                    print(f"         🔄 {SITES[site_key]['name']}: Mise à jour cotes")
            
            task = self.get_match_details(
                site_key, 
                match_info["sport_id"], 
                match_info["competition_id"], 
                match_info["match_id"]
            )
            tasks.append(task)
            sites_to_capture.append(site_key)
        
        if not tasks:
            return
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for site_key, result in zip(sites_to_capture, results):
            has_cache = external_id in self.captured_odds and site_key in self.captured_odds[external_id]
            
            if isinstance(result, Exception) or not result:
                if has_cache:
                    print(f"         ⚠️  {SITES[site_key]['name']}: Échec update → conservation cache")
                else:
                    print(f"         ❌ {SITES[site_key]['name']}: Pas de cotes (déjà disparu)")
                
                error_msg = f"{type(result).__name__}" if isinstance(result, Exception) else "Pas de réponse"
                self.api_health.record_getmatch(site_key, False, error_msg)
                continue
            
            odds = self.extract_odds(result, site_key)
            
            if odds and odds.get("markets"):
                was_updated = has_cache
                
                self.captured_odds[external_id][site_key] = odds
                
                if was_updated:
                    print(f"         ✅ {SITES[site_key]['name']}: {len(odds['markets'])} marchés ACTUALISÉS !")
                else:
                    print(f"         ✅ {SITES[site_key]['name']}: {len(odds['markets'])} marchés CAPTURÉS !")
                
                # Sauvegarde (throttled)
                self.save_cache_to_disk()
                
                self.api_health.record_getmatch(site_key, True)
            else:
                self.api_health.record_getmatch(site_key, False, "Pas de marchés")
    
    async def check_matches_for_finalization(self):
        """Vérifier quels matchs doivent être finalisés (INCLUANT les déjà commencés)"""
        
        all_match_ids = set(self.matches_by_external_id.keys()) | set(self.matches_info_archive.keys())
        
        for external_id in all_match_ids:
            if external_id in self.completed_matches:
                continue
            
            all_sites_for_match = set()
            if external_id in self.matches_by_external_id:
                all_sites_for_match = set(self.matches_by_external_id[external_id].keys())
            elif external_id in self.matches_info_archive:
                all_sites_for_match = set(self.matches_info_archive[external_id].keys())
            
            if not all_sites_for_match:
                continue
            
            all_closed = self.closed_sites[external_id] | self.early_closed.get(external_id, set())
            
            if all_closed >= all_sites_for_match:
                match_info = self.matches_info_archive.get(external_id)
                if not match_info:
                    continue
                
                first_match = list(match_info.values())[0]
                start_time = first_match.get("start_time", "")
                time_until = self._get_time_until_match(start_time)
                
                if time_until is not None and time_until <= 0:
                    priority = "urgent" if time_until < 5 else "normal"
                    
                    # Anti double-finalisation: ne pas re-queue si déjà en cours ou déjà dans la file
                    if external_id in self.finalizing_in_progress or external_id in self.finalization_queue:
                        continue
                    
                    self.finalization_queue.add_match(external_id, priority)
    
    async def finalize_multiple_matches_batch(self, external_ids: List[int]):
        """
        Finaliser plusieurs matchs + gestion matchs sans cotes.
        Ajoute les matchs à la queue de résultats pour récupération automatique.
        """
        
        all_sheets_data = defaultdict(list)
        matches_to_queue_for_results = []  # Matchs à ajouter à la queue des résultats
        
        for external_id in external_ids:
            # VÉRIFIER D'ABORD LE CACHE
            has_cached_odds = (external_id in self.captured_odds and 
                              len(self.captured_odds[external_id]) > 0)
            
            if not has_cached_odds:
                # Vraiment aucune cote (ni cache ni nouveau)
                print(f"      ⚠️  Match {external_id} : Aucune cote capturée → ajouté en retry")
                
                # Stocker pour retry au lieu de supprimer
                matches_info = self.matches_by_external_id.get(external_id) or self.matches_info_archive.get(external_id)
                
                if matches_info:
                    first_match = list(matches_info.values())[0]
                    
                    if external_id not in self.matches_without_odds_retry:
                        self.matches_without_odds_retry[external_id] = {
                            "match_info": matches_info,
                            "retry_count": 0,
                            "last_attempt": now_mauritius(),
                            "match_name": first_match.get("match_name", ""),
                            "start_time": first_match.get("start_time", "")
                        }
                    else:
                        self.matches_without_odds_retry[external_id]["retry_count"] += 1
                
                # Sauvegarde (throttled)
                self.save_cache_to_disk()
                continue
            
            # On a des cotes en cache → Utiliser pour Google Sheets
            cached_sites = list(self.captured_odds[external_id].keys())
            print(f"      ✅ Match {external_id} : {len(cached_sites)} site(s) avec cotes (cache)")
            
            matches_info = None
            if external_id in self.matches_by_external_id and self.matches_by_external_id[external_id]:
                matches_info = self.matches_by_external_id[external_id]
            elif external_id in self.matches_info_archive:
                matches_info = self.matches_info_archive[external_id]
            
            if not matches_info:
                continue
            
            try:
                sheets_data = self._prepare_sheets_data(external_id, matches_info)
                
                if not sheets_data:
                    continue
                
                for sheet_name, rows in sheets_data.items():
                    all_sheets_data[sheet_name].extend(rows)
                
                # Préparer les données pour la queue de résultats
                matches_to_queue_for_results.append({
                    "external_id": external_id,
                    "matches_info": matches_info.copy()
                })
            
            except Exception:
                pass
        
        # Si aucune donnée, sortir
        if not all_sheets_data:
            print(f"      ⚠️  Aucune donnée à envoyer dans ce batch")
            return
        
        try:
            gsheets_success = await self.gsheets.append_rows_batch(dict(all_sheets_data))
            
            self.gsheets.update_summary(force=False)
            
            self._write_to_local_excel(dict(all_sheets_data))
            
            if gsheets_success:
                for external_id in external_ids:
                    # Seulement les matchs AVEC cotes
                    if external_id in self.captured_odds and len(self.captured_odds[external_id]) > 0:
                        self.completed_matches.add(external_id)
                        
                        if external_id in self.matches_info_archive:
                            del self.matches_info_archive[external_id]
                        if external_id in self.captured_odds:
                            del self.captured_odds[external_id]
                        if external_id in self.closed_sites:
                            del self.closed_sites[external_id]
                
                # ===== NOUVEAU: Ajouter les matchs à la queue des résultats =====
                for match_data in matches_to_queue_for_results:
                    external_id = match_data["external_id"]
                    matches_info = match_data["matches_info"]
                    
                    # Stocker les métadonnées pour récupération résultats
                    self.matches_info_archive_for_results[external_id] = matches_info
                    
                    # Calculer l'heure de fin estimée (match + 2h de jeu)
                    first_match = list(matches_info.values())[0]
                    start_time_str = first_match.get("start_time", "")
                    
                    try:
                        match_start = date_parser.parse(start_time_str.replace(',', '').strip())
                        if match_start.tzinfo is None:
                            match_start = match_start.replace(tzinfo=MAURITIUS_TZ)
                        estimated_end = match_start + timedelta(hours=2)  # Match dure ~2h
                    except:
                        estimated_end = now_mauritius() + timedelta(hours=2)
                    
                    # Ajouter à la queue des résultats
                    if external_id not in self.results_queue:
                        self.results_queue.add_match(external_id, matches_info, estimated_end)
                        print(f"      📊 Match {external_id}: Ajouté à la queue résultats (disponible après {estimated_end.strftime('%H:%M')})")
                
                # Sauvegarde immédiate après finalisation
                self.save_cache_to_disk(force=True)
        
        except Exception as e:
            print(f"      ❌ Erreur finalisation batch: {e}")
    
    def _prepare_sheets_data(self, external_id: int, matches_info: Dict[str, dict]) -> Dict[str, List[Dict]]:
        """
        Préparer les données pour Google Sheets.
        
        Colonnes finales (ordre):
        - Date, Heure, External ID, Match, Compétition, Heure Match
        - StevenHills, SuperScore, ToteLePEP, PlayOnline (cotes par site)
        - Score, Résultat (vides initialement, mis à jour après 3h)
        - MatchId_SH, CompId_SH, SportId_SH (métadonnées StevenHills)
        - MatchId_SS, CompId_SS, SportId_SS (métadonnées SuperScore)
        - MatchId_TP, CompId_TP, SportId_TP (métadonnées ToteLePEP)
        - MatchId_PO, CompId_PO, SportId_PO (métadonnées PlayOnline)
        """
        
        first_match = list(matches_info.values())[0]
        
        competition_name = first_match.get("competition_name", "")
        if not competition_name and external_id in self.captured_odds:
            for site_odds in self.captured_odds[external_id].values():
                if "competition_name" in site_odds and site_odds["competition_name"]:
                    competition_name = site_odds["competition_name"]
                    break
        
        # Données de base
        base_data = {
            "Date": now_mauritius_str("%Y-%m-%d"),
            "Heure": now_mauritius_str("%H:%M:%S"),
            "External ID": external_id,
            "Match": first_match["match_name"],
            "Compétition": competition_name,
            "Heure Match": first_match["start_time"],
        }
        
        all_market_keys = set()
        for site_odds in self.captured_odds[external_id].values():
            all_market_keys.update(site_odds.get("markets", {}).keys())
        
        sheets_data = {}
        
        # Mapping des sites vers leurs préfixes de colonnes métadonnées
        site_prefixes = {
            "stevenhills": "SH",
            "superscore": "SS",
            "totelepep": "TP",
            "playonlineltd": "PO"
        }
        
        for market_key in all_market_keys:
            sheet_name = MARKET_SHEET_MAPPING.get(market_key, market_key[:31])
            
            row = base_data.copy()
            
            # Ajouter les cotes par site
            for site_key in SITES.keys():
                site_name = SITES[site_key]["name"]
                
                if site_key in self.captured_odds[external_id]:
                    markets = self.captured_odds[external_id][site_key].get("markets", {})
                    
                    if market_key in markets:
                        odds_str = self._format_odds_for_display(markets[market_key].get("odds", {}))
                        row[site_name] = odds_str
                    else:
                        row[site_name] = ""
                else:
                    row[site_name] = ""
            
            # Ajouter colonnes Score et Résultat (vides initialement)
            row["Score"] = ""
            row["Résultat"] = ""
            
            # Ajouter colonnes métadonnées cachées pour chaque site
            for site_key, prefix in site_prefixes.items():
                if site_key in matches_info:
                    site_match = matches_info[site_key]
                    row[f"MatchId_{prefix}"] = site_match.get("match_id", "")
                    row[f"CompId_{prefix}"] = site_match.get("competition_id", "")
                    row[f"SportId_{prefix}"] = 1  # Soccer = 1
                else:
                    row[f"MatchId_{prefix}"] = ""
                    row[f"CompId_{prefix}"] = ""
                    row[f"SportId_{prefix}"] = ""
            
            if sheet_name not in sheets_data:
                sheets_data[sheet_name] = []
            sheets_data[sheet_name].append(row)
        
        return sheets_data
    
    def _write_to_local_excel(self, sheets_data: Dict[str, List[dict]]):
        """Écrire dans Excel local"""
        
        if self.local_cumulative_excel.exists():
            try:
                existing_sheets = pd.read_excel(self.local_cumulative_excel, sheet_name=None, engine='openpyxl')
            except:
                existing_sheets = {}
        else:
            existing_sheets = {}
        
        all_sheet_names = set(existing_sheets.keys()) | set(sheets_data.keys())
        all_sheet_names.discard("Summary")
        
        with pd.ExcelWriter(self.local_cumulative_excel, engine='openpyxl', mode='w') as writer:
            
            summary_data = [
                ["ALL MATCHES CUMULATIVE - LOCAL"],
                ["Dernière mise à jour", now_mauritius_str()],
                ["Fuseau horaire", "Indian/Mauritius (UTC+4)"],
                ["Utilisateur", "antema102"],
                [""],
                ["Feuilles disponibles:"],
            ]
            
            for sheet_name in sorted(all_sheet_names):
                summary_data.append([f"  - {sheet_name}"])

            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False, header=False)
            
            for sheet_name in sorted(all_sheet_names):
                existing_df = existing_sheets.get(sheet_name, pd.DataFrame())
                new_rows = sheets_data.get(sheet_name, [])
                new_df = pd.DataFrame(new_rows)
                
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    def _format_odds_for_display(self, market_odds: dict) -> str:
        sorted_opts = sorted(market_odds.items(), key=lambda x: x[1].get("option_no", 999))
        odds_values = [f"{opt_data.get('odd', 0):.2f}" for _, opt_data in sorted_opts]
        return " / ".join(odds_values)
    
    async def process_finalization_queue(self):
        """Processeur de queue (tâche background)"""
        
        while True:
            try:
                batch = self.finalization_queue.get_next_batch()
                
                if batch:
                    print(f"\n{'='*70}")
                    print(f"📦 TRAITEMENT BATCH ({len(batch)} matchs)")
                    print(f"   Queue restante : {len(self.finalization_queue)} matchs")
                    print(f"{'='*70}")
                    
                    # Marquer comme en cours de finalisation
                    for mid in batch:
                        self.finalizing_in_progress.add(mid)
                    
                    try:
                        await self.finalize_multiple_matches_batch(batch)
                    finally:
                        # Libérer les verrous même en cas d'erreur
                        for mid in batch:
                            self.finalizing_in_progress.discard(mid)
                    
                    print(f"   ✅ Batch traité")
                
                await asyncio.sleep(1)
            
            except Exception as e:
                print(f"❌ Erreur processeur queue: {e}")
                await asyncio.sleep(5)
    
    def _check_date_change(self):
        """Vérifier changement de date"""
        current_date = now_mauritius_date_str()
        
        if current_date != self.current_date:
            print(f"\n{'='*70}")
            print(f"🌙 CHANGEMENT DE DATE DÉTECTÉ")
            print(f"{'='*70}")
            print(f"   Ancienne : {self.current_date}")
            print(f"   Nouvelle : {current_date}")
            
            self.matches_by_external_id.clear()
            self.captured_odds.clear()
            self.closed_sites.clear()
            self.completed_matches.clear()
            self.matches_info_archive.clear()
            self.matches_snapshot.clear()
            self.early_closed.clear()
            self.finalizing_in_progress.clear()
            
            # NOTE: On ne réinitialise PAS results_queue et matches_info_archive_for_results
            # car les résultats des matchs de la veille peuvent encore être récupérés
            # (3h+ après la fin du match)
            
            self.current_date = current_date
            
            print(f"   ✅ Données réinitialisées pour {current_date}")
            print(f"   📊 Queue résultats conservée : {len(self.results_queue)} matchs en attente")
            print(f"{'='*70}\n")
            # Sauvegarde l'état réinitialisé
            self.save_cache_to_disk(force=True)
    
    async def retry_matches_without_odds(self):
        """Retenter la capture des matchs sans cotes"""
        
        if not self.matches_without_odds_retry:
            return
        
        print(f"\n   🔁 Retry matchs sans cotes ({len(self.matches_without_odds_retry)} matchs)")
        
        matches_to_retry = list(self.matches_without_odds_retry.keys())
        
        for external_id in matches_to_retry:
            retry_info = self.matches_without_odds_retry[external_id]
            matches_info = retry_info["match_info"]
            retry_count = retry_info["retry_count"]
            
            # Abandonner si max retry ou match déjà joué
            time_until = self._get_time_until_match(retry_info["start_time"])
            
            if time_until is not None and time_until < -120:
                print(f"      ⏭️  Match {external_id} : Déjà joué → abandon")
                del self.matches_without_odds_retry[external_id]
                self.completed_matches.add(external_id)
                self.save_cache_to_disk()
                continue
            
            if retry_count >= self.max_retry_attempts:
                print(f"      ❌ Match {external_id} : Max retry atteint → abandon")
                del self.matches_without_odds_retry[external_id]
                self.completed_matches.add(external_id)
                self.save_cache_to_disk()
                continue
            
            # Retenter capture
            sites_to_retry = set(matches_info.keys())
            
            for site_key in sites_to_retry:
                match_info = matches_info[site_key]
                data = await self.get_match_details(
                    site_key,
                    match_info["sport_id"],
                    match_info["competition_id"],
                    match_info["match_id"]
                )
                
                if data:
                    odds = self.extract_odds(data, site_key)
                    if odds and odds.get("markets"):
                        self.captured_odds[external_id][site_key] = odds
                        print(f"         ✅ {SITES[site_key]['name']}: Cotes trouvées !")
                        del self.matches_without_odds_retry[external_id]
                        self.finalization_queue.add_match(external_id, "urgent")
                        self.save_cache_to_disk()
                        break
    
    async def resync_from_excel_to_gsheets(self):
        """Resynchroniser Google Sheets depuis Excel local"""
        
        print(f"\n{'='*70}")
        print(f"🔄 RESYNCHRONISATION Google Sheets depuis Excel")
        print(f"{'='*70}")
        
        try:
            excel_path = self.local_cumulative_excel
            
            if not excel_path.exists():
                print(f"❌ Fichier Excel introuvable : {excel_path}")
                return False
            
            print(f"📂 Lecture Excel : {excel_path.name}")
            
            excel_sheets = pd.read_excel(excel_path, sheet_name=None, engine='openpyxl')
            
            self.gsheets.last_row_cache.clear()
            
            total_synced = 0
            total_feuilles = 0
            
            for sheet_name, df in excel_sheets.items():
                if sheet_name == "Summary":
                    continue
                
                if df.empty:
                    print(f"\n   ⏭️  '{sheet_name}' : vide (ignoré)")
                    continue
                
                total_feuilles += 1
                
                # PAUSE tous les 5 feuilles
                if total_feuilles > 0 and total_feuilles % 5 == 0:
                    print(f"\n   ⏸️  Pause de 60s (quota protection - {total_feuilles} feuilles traitées)...")
                    await asyncio.sleep(60)
                
                print(f"\n   📋 '{sheet_name}'...")
                
                worksheet = self.gsheets.get_or_create_worksheet(sheet_name)
                if not worksheet:
                    print(f"      ❌ Impossible de créer/obtenir la feuille")
                    continue
                
                try:
                    def get_all():
                        return worksheet.get_all_values()
                    
                    gs_values = self.gsheets._execute_with_retry(get_all)
                    gs_row_count = len(gs_values)
                except Exception as e:
                    print(f"      ⚠️  Erreur lecture : {e}")
                    gs_row_count = 0
                
                excel_row_count = len(df) + 1
                
                print(f"      📊 Google Sheets : {gs_row_count} lignes")
                print(f"      📊 Excel : {excel_row_count} lignes")
                
                if excel_row_count > gs_row_count:
                    missing = excel_row_count - gs_row_count
                    print(f"      ⚠️  {missing} lignes manquantes !")
                    print(f"      🔄 Réécriture complète...")
                    
                    header = list(df.columns)
                    data_rows = df.values.tolist()
                    
                    data_rows = [['' if pd.isna(val) else val for val in row] for row in data_rows]
                    
                    all_data = [header] + data_rows
                    
                    try:
                        def clear_sheet():
                            return worksheet.clear()
                        
                        self.gsheets._execute_with_retry(clear_sheet)
                        print(f"      🧹 Feuille effacée")
                    except Exception as e:
                        print(f"      ⚠️  Erreur effacement : {e}")
                    
                    # Pause
                    await asyncio.sleep(3)
                    
                    chunk_size = 1000
                    
                    for i in range(0, len(all_data), chunk_size):
                        chunk = all_data[i:i+chunk_size]
                        start_row = i + 1
                        
                        try:
                            def write_chunk():
                                range_name = f'A{start_row}'
                                return worksheet.update(values=chunk, range_name=range_name)
                            
                            self.gsheets._execute_with_retry(write_chunk)
                            
                            print(f"      ✅ Lignes {start_row}-{start_row+len(chunk)-1} écrites")
                            
                        except Exception as e:
                            print(f"      ❌ Erreur écriture chunk : {e}")
                            continue
                        
                        if i + chunk_size < len(all_data):
                            await asyncio.sleep(3)
                    
                    total_synced += len(data_rows)
                    print(f"      ✅ {len(data_rows)} lignes synchronisées")
                    
                    await asyncio.sleep(3)
                    
                else:
                    print(f"      ✅ Déjà synchronisé")
            
            print(f"\n{'='*70}")
            print(f"✅ Resynchronisation terminée")
            print(f"   📊 {total_feuilles} feuilles traitées")
            print(f"   ➕ {total_synced} lignes ajoutées au total")
            print(f"{'='*70}\n")
            
            # Mettre à jour Summary si quota OK
            if self.gsheets.api_call_count < 50:
                print("📝 Mise à jour Summary...")
                self.gsheets.update_summary(force=True)
            else:
                print("📝 Summary sera mis à jour lors de la prochaine itération (quota élevé)")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erreur resynchronisation : {e}")
            traceback.print_exc()
            return False
    
    async def run_tracking(self, sport="Soccer", interval_seconds=120):
        print("=" * 70)
        print("🎯 MULTI-SITES ODDS TRACKER - VERSION FINALE + RÉSULTATS AUTO")
        print("=" * 70)
        print(f"Sites: {', '.join([s['name'] for s in SITES.values()])}")
        print(f"Google Sheets: {GOOGLE_SHEETS_CONFIG['sheet_name']}")
        print(f"Excel local: {self.local_cumulative_excel.name}")
        print(f"📦 Batch size: {self.finalization_queue.batch_size} matchs")
        print(f"⏱️  Intervalle batch: {self.finalization_queue.min_interval_seconds}s")
        print(f"📅 Date: {self.current_date}")
        print(f"🔄 Capture dynamique: Toutes les 2 min (matchs < 60 min)")
        print(f"📸 Capture avant disparition: Immédiate")
        print(f"🆔 Identifiant: externalId (index 28)")
        print(f"📊 Résultats: Récupération auto {RESULTS_DELAY_HOURS}h après fin match")
        print(f"🔁 Check résultats: Toutes les {RESULTS_CHECK_INTERVAL_HOURS}h (iteration % 30)")
        print(f"Fuseau: Maurice (UTC+4)")
        print(f"Utilisateur: antema102")
        print(f"✅ V2.5: Résultats automatiques + métadonnées")
        print("=" * 70)
        print()
        
        if len(self.completed_matches) > 0 or self.local_cumulative_excel.exists():
            print(f"⚠️  Fichier Excel détecté")
            
            resync_choice = input("\n🔄 Resynchroniser Google Sheets depuis Excel ? (O/n) : ").strip().lower()
            
            if resync_choice != 'n':
                print("\n🚀 Lancement resynchronisation...")
                success = await self.resync_from_excel_to_gsheets()
                
                if success:
                    print("✅ Resynchronisation terminée avec succès !")
                    input("\n⏸️  Appuyez sur ENTRÉE pour continuer le tracking...")
                else:
                    print("⚠️  Resynchronisation avec erreurs")
                    choice = input("\n❓ Continuer quand même ? (O/n) : ").strip().lower()
                    if choice == 'n':
                        return
        
        queue_task = asyncio.create_task(self.process_finalization_queue())
        
        try:
            while True:
                self.iteration += 1
                
                print(f"\n{'='*70}")
                print(f"🔄 ITERATION #{self.iteration} - {now_mauritius_str()}")
                print(f"{'='*70}")
                
                self._check_date_change()
                
                should_full_getsport = (self.iteration % 5 == 1)
                
                if should_full_getsport:
                    print(f"\n📡 GetSport COMPLET (pagination parallèle)")
                    matches_by_site = await self.get_all_sites_matches(self.current_date)
                    
                    for site_key, matches in matches_by_site.items():
                        for external_id, match_info in matches.items():
                            if external_id not in self.matches_info_archive:
                                self.matches_info_archive[external_id] = {}
                            self.matches_info_archive[external_id][site_key] = match_info
                            
                            if external_id not in self.matches_by_external_id:
                                self.matches_by_external_id[external_id] = {}
                            self.matches_by_external_id[external_id][site_key] = match_info
                    
                    await self.detect_early_closures_and_reopenings(matches_by_site)
                    
                    self.last_getsport_full = now_mauritius()
                    
                    self.gsheets.invalidate_cache()
                
                else:
                    print(f"\n🔍 Vérification LÉGÈRE (matchs proches)")
                
                await self.verify_close_matches_availability()
                
                # Retry matchs sans cotes toutes les 2 itérations
                if self.iteration % 2 == 0:
                    await self.retry_matches_without_odds()
                
                await self.check_matches_for_finalization()
                
                # ===== NOUVEAU: Check résultats toutes les heures (30 itérations x 2min = 1h) =====
                if self.iteration % 30 == 0:
                    await self.check_and_update_results()
                
                print(f"\n   📊 Matchs suivis : {len(self.matches_info_archive)}")
                print(f"   ✅ Matchs complétés : {len(self.completed_matches)}")
                print(f"   📦 Queue cotes : {len(self.finalization_queue)} matchs")
                print(f"   📊 Queue résultats : {len(self.results_queue)} matchs")
                print(f"   💾 Cotes en cache : {sum(len(sites) for sites in self.captured_odds.values())} sites")

                # Rapport santé toutes les 5 itérations
                if self.iteration % 5 == 0:
                    self.api_health.print_report()

                # Reset stats si nécessaire
                self.api_health.reset_if_needed()
                
                # Sauvegarde périodique de l'état
                self.save_cache_to_disk()
                
                print(f"\n   ⏳ Prochaine itération dans {interval_seconds}s...")
                await asyncio.sleep(interval_seconds)
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Arrêt manuel")
            queue_task.cancel()
        
        finally:
            # Sauvegarde finale
            self.save_cache_to_disk(force=True)
            print("\n✅ Arrêt du script")
            print(f"📊 Queue cotes finale : {len(self.finalization_queue)} matchs")
            print(f"📊 Queue résultats finale : {len(self.results_queue)} matchs")


async def main():
    async with MultiSitesOddsTrackerFinal(output_dir="multi_sites_odds") as tracker:
        await tracker.run_tracking(
            sport="Soccer",
            interval_seconds=120
        )


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 TRACKER MULTI-SITES - VERSION FINALE + RÉSULTATS AUTO")
    print("=" * 70)
    print(f"📅 Date: {now_mauritius_str('%Y-%m-%d')}")
    print(f"🕐 Heure Maurice: {now_mauritius_str()}")
    print(f"👤 Utilisateur: antema102")
    print(f"✅ V2.5: Résultats automatiques + métadonnées")
    print(f"📊 Délai résultats: {RESULTS_DELAY_HOURS}h après fin match")
    print(f"🔁 Max tentatives: {MAX_RESULTS_RETRY}")
    print("=" * 70)
    print()
    asyncio.run(main())