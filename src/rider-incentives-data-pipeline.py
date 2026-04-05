"""
End-to-end data pipeline that processes rider performance data,
assigns incentive tiers, and triggers notifications via webhook.

Simulates a real-world data engineering use case.
"""

import requests
import json

class RiderNotificationSystem:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self.rider_data_url = "https://gist.githubusercontent.com/alfiecairns/0e5d08a3a1c0b2574e794b4a428e705e/raw/29a2e46c771901a6af7da8e60a50531b59498493/bc_rider_data.json"
        self.promo_data_url = "https://gist.githubusercontent.com/alfiecairns/777902d54e69e186e82a42944559cd55/raw/bbf8c410273ae76d5923f61514d24bf3d2d4f641/bc_promocodes.json"
        
        
    def fetch_data(self):
        # Fetch rider data and promo codes
        try:
            # Fetch rider data
            print("Fetching rider data...")
            rider_response = requests.get(self.rider_data_url)
            rider_response.raise_for_status()
            rider_data = rider_response.json()
            print(f"Retrieved {len(rider_data)} riders")
            
            # Fetch promo codes data
            print("Fetching promo codes...")
            promo_response = requests.get(self.promo_data_url)
            promo_response.raise_for_status()
            promo_data = promo_response.json()
            print(f"Retrieved promo codes for {len(promo_data)} cities")
            
            return rider_data, promo_data
            
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return [], {}
    
    def check_eligibility(self, rider):
        """Check if a rider meets all eligibility criteria."""
        required_fields = [
            'status', 'weekly_hours', 'total_orders_weekly', 
            'acceptance_rate', 'reassignment_rate', 'customer_rating', 
            'avg_delivery_time_mins'
        ]
        
        # Check for missing fields
        for field in required_fields:
            if field not in rider or rider[field] is None:
                return False
        
        # Check for string instead of number in weekly_hours
        if isinstance(rider['weekly_hours'], str):
            try:
                rider['weekly_hours'] = float(rider['weekly_hours'])
            except ValueError:
                return False
        
        # Check eligibility criteria
        return (rider['status'] == 'active' and
                rider['weekly_hours'] >= 25 and
                rider['total_orders_weekly'] >= 50 and
                rider['acceptance_rate'] >= 0.88 and
                rider['reassignment_rate'] <= 0.05 and
                rider['customer_rating'] >= 4.75 and
                rider['avg_delivery_time_mins'] <= 30)
    
    def classify_tier(self, rider):
        # Classify rider into appropriate tier based on performance
        orders = rider['total_orders_weekly']
        rating = rider['customer_rating']
        
        if orders > 90 and rating >= 4.9:
            return "Platinum"
        elif orders > 75 and rating >= 4.8:
            return "Gold"
        elif orders > 60:
            return "Silver"
        else:
            return "Bronze"
    
    def format_rider_name(self, full_name):
        #Format rider name 
        names = full_name.split()
        if len(names) >= 2:
            last_name = names[-1]
            first_initial = names[0][0].upper() + "."
            return f"{last_name}, {first_initial}"
        return full_name
    
    def get_template_name(self, tier):
        # Get the correct template name for each tier
        templates = {
            "Platinum": "GLO_RIDER_BONUS_PLATINUM_V1",
            "Gold": "GLO_RIDER_BONUS_GOLD_V1",
            "Silver": "GLO_RIDER_BONUS_SILVER_V1",
            "Bronze": "GLO_RIDER_BONUS_BRONZE_V1"
        }
        return templates.get(tier, "")
    
    def organize_promo_codes(self, promo_data):
        # Organize promo codes by tier and city 
        organized_codes = {}
        
        for city, tiers in promo_data.items():
            for tier, codes in tiers.items():
                if tier not in organized_codes:
                    organized_codes[tier] = {}
                if city not in organized_codes[tier]:
                    organized_codes[tier][city] = []
                
                # add each code with status "used"
                for code in codes:
                    organized_codes[tier][city].append({
                        'code': code,
                        'used': False
                    })
        
        return organized_codes
    
    def assign_promo_code(self, organized_codes, tier, city):
        #Assign the first available promo code for the given tier and city
        if tier not in organized_codes or city not in organized_codes[tier]:
            return None
        
        available_codes = organized_codes[tier][city]
        
        for promo in available_codes:
            if not promo['used']:
                promo['used'] = True
                return promo['code']
        
        return None
    
    def send_notification(self, rider, tier, promo_code):
        #Send notification to rider via POST request
        template_name = self.get_template_name(tier)
        formatted_name = self.format_rider_name(rider['name'])
        
        payload = {
            "template_name": template_name,
            "rider_id": rider['rider_id'],  
            "placeholders": {
                "rider_name": formatted_name,
                "city_name": rider['city'],
                "promocode": promo_code
            }
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            response.raise_for_status()
            print(f"Notification sent for rider {rider['rider_id']} ({tier} tier)")
            return True
            
        except requests.RequestException as e:
            print(f"Failed to send notification for rider {rider['rider_id']}: {e}")
            return False
    
    def process_riders(self):
        # Main method to process all riders and send notifications
        rider_data, promo_data = self.fetch_data()
        
        if not rider_data or not promo_data:
            print("Failed to fetch required data")
            return {"total_processed": 0, "eligible": 0, "notifications_sent": 0}
        
        # Organize promo codes
        organized_codes = self.organize_promo_codes(promo_data)
        
        # Process riders
        eligible_count = 0
        notifications_sent = 0
        
        print(f"Processing {len(rider_data)} riders...")
        
        for rider in rider_data:
            # Check eligibility
            if not self.check_eligibility(rider):
                continue
            
            eligible_count += 1
            
            # Classify tier
            tier = self.classify_tier(rider)
            
            # Assign promo code
            promo_code = self.assign_promo_code(organized_codes, tier, rider['city'])
            
            if not promo_code:
                print(f"No promo code available for rider {rider['rider_id']} ({tier} tier, {rider['city']})")
                continue
            
            # Send notification
            if self.send_notification(rider, tier, promo_code):
                notifications_sent += 1
        
        # Print summary
        print("\n" + "="*50)
        print("PROCESSING SUMMARY")
        print("="*50)
        print(f"Total riders processed: {len(rider_data)}")
        print(f"Eligible riders: {eligible_count}")
        print(f"Notifications sent: {notifications_sent}")
        print("="*50)
        
        return {
            "total_processed": len(rider_data),
            "eligible": eligible_count,
            "notifications_sent": notifications_sent
        }


def main():

    WEBHOOK_URL = "https://webhook.site/1d647ab2-d3f7-400b-8a13-3d4bf94f4071"
    
    # Init the notification system
    notification_system = RiderNotificationSystem(WEBHOOK_URL)
    
    # Process all riders
    results = notification_system.process_riders()
    
    # Print final results
    print(f"\n Final Results:")
    print(f"Successfully processed {results['total_processed']} riders")
    print(f"Found {results['eligible']} eligible riders")
    print(f"Sent {results['notifications_sent']} notifications")


if __name__ == "__main__":
    main()
