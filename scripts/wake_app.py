import asyncio
import sys

from playwright.async_api import async_playwright


async def wake_app():
    url = "https://india-fund.streamlit.app/"
    print(f"Starting wake-up routine for {url}")

    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        page = await context.new_page()

        try:
            print(f"Navigating to {url}...")
            await page.goto(url, wait_until="networkidle", timeout=60000)

            # Check for the "Yes, get this app back up!" button
            # We look for the text specifically
            wake_button_selector = "text='Yes, get this app back up!'"

            if await page.is_visible(wake_button_selector):
                print("App is asleep. Clicking wake-up button...")
                await page.click(wake_button_selector)
                print("Clicked wake-up button. Waiting for app to load...")
                # Wait a bit for it to start working
                await asyncio.sleep(10)
                print("Wake-up signal sent successfully.")
            else:
                # Also check for common streamlit error/sleep patterns
                content = await page.content()
                if "Your app is in the oven" in content:
                    print("App is currently waking up (in the oven).")
                elif "MF Analytics" in content:
                    print("App is already awake and loaded.")
                else:
                    print("App seems to be awake (or in an unknown state).")

            # Final check - take a screenshot if needed for debugging (optional in GH actions)
            # await page.screenshot(path="app_state.png")

        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit(0)  # Exit with 0 anyway to not fail the job, or 1 if you want to be notified
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(wake_app())
