"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.registry = registry
        self.auto_offset_reset = "earliest"  # Lire depuis le début
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
        self.events_history = []  # Liste pour stocker l'historique des événements
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur historique : {self.group_id}")
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,  # earliest pour lire depuis le début
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  
            enable_auto_commit=True
        )
        
        try:
            self.logger.info("Lecture de l'historique des événements depuis le début...")
            
            # Utiliser poll() avec un timeout pour lire tous les messages disponibles
            timeout_ms = 5000  # 5 secondes de timeout
            while True:
                message_batch = self.consumer.poll(timeout_ms=timeout_ms)
                
                if not message_batch:
                    # Aucun nouveau message après le timeout
                    self.logger.info(f"Aucun nouveau message après {timeout_ms/1000}s. Fin de la lecture de l'historique.")
                    break
                
                # Traiter tous les messages du batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        event_data = message.value
                        self._process_historical_message(event_data)
                    
        except KeyboardInterrupt:
            self.logger.info("Arrêt du consommateur historique demandé par l'utilisateur")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de l'historique: {e}", exc_info=True)
        finally:
            self._save_events_to_file()
            self.stop()

    def _process_historical_message(self, event_data: dict) -> None:
        """Process and store a historical message"""
        event_type = event_data.get('event')
        
        if not event_type:
            self.logger.warning(f"Message historique manque le champ 'event': {event_data}")
            return
        
        # Ajouter l'événement à l'historique
        self.events_history.append(event_data)
        self.logger.debug(f"Événement historique traité : {event_type} (Total: {len(self.events_history)})")
        
        # Optionnel : traiter aussi avec les handlers si nécessaire
        handler = self.registry.get_handler(event_type)
        if handler:
            try:
                handler.handle(event_data)
            except Exception as e:
                self.logger.error(f"Erreur lors du traitement de l'événement historique {event_type}: {e}")

    def _save_events_to_file(self) -> None:
        """Save all collected events to a JSON file"""
        if not self.events_history:
            self.logger.info("Aucun événement historique à sauvegarder")
            return
            
        try:
            filename = "output/events_history.json"
            # Créer le répertoire si nécessaire
            import os
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.events_history, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Historique de {len(self.events_history)} événements sauvegardé dans {filename}")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde de l'historique: {e}", exc_info=True)

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur!")