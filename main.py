from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome, ChromeOptions as Options
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent
import time

url = 'https://allslots.ro/'
options = Options()
options.add_argument("--disable-popup-blocking")
options.add_argument('--headless=new')  # Using headless mode
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')  # Important for headless on Windows

# These are critical for headless mode to work properly
options.add_argument('--window-size=1920,1080')  
options.add_argument('--start-maximized')  # Start maximized
options.add_argument('--force-device-scale-factor=1')  # Consistent scaling
options.add_argument('--hide-scrollbars=false')  # Show scrollbars

# Spoof a non-headless Chrome browser
options.add_argument('--disable-extensions')
options.add_argument('--disable-blink-features=AutomationControlled')  # Hide automation
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)

# Set a regular user agent
ua = UserAgent()
user_agent = ua.random
options.add_argument(f'user-agent={user_agent}')

service = Service(r'C:\Users\H i - G E O R G E\Documents\Airbnb Add App\chromedriver.exe')
driver = Chrome(service=service, options=options)

# Crucial: Execute this script to mask headless mode detection
driver.execute_cdp_cmd('Network.setUserAgentOverride', {
    "userAgent": user_agent,
    "platform": "Windows",
    "acceptLanguage": "en-US,en;q=0.9"
})

# Execute JS to mask WebDriver presence
driver.execute_script(
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
)

try:
    # Set generous timeouts
    driver.set_page_load_timeout(30)
    
    # Navigate to the page
    print("Navigating to the website...")
    driver.get(url)
    
    # Take screenshot immediately to verify page loading
    driver.save_screenshot("initial_load.png")
    
    # Wait for initial page load
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    print("Page loaded successfully")
    driver.save_screenshot("screenshot1.png")
    
    # Handle age verification popup - using explicit wait with longer timeout
    try:
        age_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Am 18 ani')]"))
        )
        
        # Critical for headless: ensure the button is in view before clicking
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", age_button)
        time.sleep(1)
        
        # Try JavaScript click first (more reliable in headless mode)
        driver.execute_script("arguments[0].click();", age_button)
        print("Clicked age button via JavaScript")
    except Exception as e:
        print(f"Age button not found or clickable, trying alternate approach: {e}")
        
        # Click in the center of the screen as fallback
        window_size = driver.get_window_size()
        action = ActionChains(driver)
        action.move_by_offset(int(window_width * 0.5), int(window_height * 0.5)).click().perform()
        print("Clicked center of screen")
    
    # Give page time to process the age verification
    time.sleep(5)
    driver.save_screenshot("after_age_verification.png")
    
    # Find BetMen element with explicit wait
    print("Looking for BetMen element...")
    betmen_element = WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located((By.XPATH, "//img[contains(@alt, 'Betmen') or contains(@alt, 'BetMen')]"))
    )
    
    # Critical for headless: ensure element is in viewport
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", betmen_element)
    time.sleep(3)  # Give time to scroll and render
    driver.save_screenshot("betmen_visible.png")
    
    # Use a more direct JavaScript approach to find stars - less prone to headless issues
    print("Finding stars with JavaScript...")
    stars_found = driver.execute_script("""
        const betmenEl = arguments[0];
        
        // First look for parent with stars
        let container = betmenEl;
        let starsArray = [];
        
        // Go up to 5 levels up looking for stars
        for (let i = 0; i < 5; i++) {
            if (!container.parentElement) break;
            container = container.parentElement;
            
            // Look for star elements
            const stars = container.querySelectorAll('[class*="star"], [class*="rmp-icon--star"]');
            if (stars && stars.length > 0) {
                // Convert NodeList to array and store references
                starsArray = Array.from(stars);
                container.style.border = '2px solid red';  // Mark container
                break;
            }
        }
        
        // If we found stars, mark the last one and return its index
        if (starsArray.length > 0) {
            const lastStar = starsArray[starsArray.length - 1];
            lastStar.style.border = '3px solid green';
            
            // Return the index in page context so we can find it later
            const allElements = document.querySelectorAll('*');
            return Array.from(allElements).indexOf(lastStar);
        }
        
        return -1;  // No stars found
    """, betmen_element)
    
    print(f"Stars search result: {stars_found}")
    driver.save_screenshot("stars_highlighted.png")
    
    if stars_found >= 0:
        # Use the element index to find the star
        print("Star found, preparing to click...")
        
        # First check and close any modal dialogs
        modals = driver.find_elements(By.CSS_SELECTOR, ".dialog-widget, .popup-modal, .modal")
        for modal in modals:
            if modal.is_displayed():
                print("Found modal dialog - closing it first")
                try:
                    close_buttons = modal.find_elements(By.CSS_SELECTOR, ".dialog-close-button, .close")
                    if close_buttons:
                        driver.execute_script("arguments[0].click();", close_buttons[0])
                    else:
                        # Click top-right corner which often closes modals
                        size = modal.size
                        ActionChains(driver).move_to_element(modal).move_by_offset(
                            int(size['width']/2 - 10), int(-size['height']/2 + 10)
                        ).click().perform()
                except Exception as modal_error:
                    print(f"Error closing modal: {modal_error}")
                time.sleep(2)
        
        # Get the star element again by index
        target_star = driver.execute_script("""
            const allElements = document.querySelectorAll('*');
            const starEl = allElements[arguments[0]];
            
            // Make it very visible and clickable
            starEl.style.position = 'relative';
            starEl.style.zIndex = '9999';
            starEl.scrollIntoView({block: 'center'});
            
            return starEl;
        """, stars_found)
        
        time.sleep(1)
        driver.save_screenshot("before_star_click.png")
        
        # Use JavaScript click - most reliable in headless mode
        print("Clicking star with JavaScript...")
        driver.execute_script("arguments[0].click();", target_star)
        
        time.sleep(2)
        driver.save_screenshot("after_star_click.png")
        print("Star clicked successfully!")
    else:
        print("No stars found to click")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    driver.save_screenshot("final_state.png")
    print("Script completed")
    # Keep browser open for inspection
    # driver.quit()
