"""
Handler: User Deleted
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from datetime import datetime
from pathlib import Path
from handlers.base import EventHandler
from typing import Dict, Any

class UserDeletedHandler(EventHandler):
    """Handles UserDeleted events"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()
    
    def get_event_type(self) -> str:
        """Return the event type this handler processes"""
        return "UserDeleted"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Create an HTML email based on user deletion data"""
        
        # Extract user data from the event
        user_id = event_data.get('id')
        name = event_data.get('name')
        email = event_data.get('email')
        deletion_date = event_data.get('datetime')
        user_type = event_data.get('type', 'client')  # Default to 'client' if not specified
        
        # Customize goodbye message based on user type
        if user_type.lower() == 'employee':
            goodbye_message = "Nous te remercions pour ton travail au sein de notre équipe. Nous te souhaitons beaucoup de succès dans tes futurs projets. N'hésite pas à rester en contact avec nous !"
        else:  # client or any other type
            goodbye_message = "Nous supprimons votre compte à votre demande. Merci d'avoir été client de notre magasin. Si jamais vous voulez créer une nouvelle compte, n'hésitez pas à nous contacter."
        
        # Get the path to the goodbye template
        current_file = Path(__file__)
        project_root = current_file.parent.parent   
        
        # Read and process the goodbye template
        with open(project_root / "templates" / "goodbye_client_template.html", 'r', encoding='utf-8') as file:
            html_content = file.read()
            html_content = html_content.replace("{{user_id}}", str(user_id))
            html_content = html_content.replace("{{name}}", name)
            html_content = html_content.replace("{{email}}", email)
            html_content = html_content.replace("{{deletion_date}}", deletion_date)
            html_content = html_content.replace("{{goodbye_message}}", goodbye_message)
        
        # Save the generated HTML to disk
        filename = os.path.join(self.output_dir, f"goodbye_{user_id}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.debug(f"Courriel HTML généré à {name} (ID: {user_id}, Type: {user_type}), {filename}")