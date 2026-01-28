from playwright.sync_api import sync_playwright, Page, Browser
from src.kpi_runners.base import BaseKPIRunner
import time
import re
import requests


class BrowserKPIRunner(BaseKPIRunner):
    """
    Browser-based KPI runner using Playwright.

    Can be used in two modes:
    1. Standalone: Creates and manages its own browser instance
    2. Shared: Uses a pre-created page instance (for batch processing)

    For shared mode, pass a page instance to run_with_page() method.
    """

    def run(self):
        """Run KPI check with its own browser instance (standalone mode)"""
        url = self.asset['url']

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )
            page = context.new_page()

            try:
                start = time.time()
                page.goto(url, timeout=15000, wait_until='networkidle')
                load_time = time.time() - start

                result = self._run_kpi_check(page, url, load_time)
            except Exception as e:
                result = {
                    "flag": True,
                    "value": None,
                    "details": str(e)
                }
            finally:
                browser.close()

            return result

    def run_with_page(self, page: Page, load_time: float = None, timeout: int = 15):
        """
        Run KPI check using a shared page instance (batch mode).
        Uses Playwright's native timeout to skip slow KPIs.

        Args:
            page: Pre-navigated Playwright page instance
            load_time: Page load time (optional, for load time KPIs)
            timeout: Max seconds for this KPI check (default: 15)

        Returns:
            dict with flag, value, details
        """
        url = self.asset['url']
        kpi_name = self.kpi.get('kpi_name', '')

        # Set Playwright's native timeout for all operations on this page
        page.set_default_timeout(timeout * 1000)

        try:
            return self._run_kpi_check(page, url, load_time or 0)
        except Exception as e:
            error_msg = str(e).lower()
            if 'timeout' in error_msg:
                return {
                    "flag": True,
                    "value": None,
                    "details": f"Skipped due to timeout ({timeout}s) - {kpi_name}"
                }
            return {
                "flag": True,
                "value": None,
                "details": f"Error: {str(e)[:200]}"
            }
        finally:
            # Restore default timeout (30s is Playwright's default)
            page.set_default_timeout(30000)

    def _run_kpi_check(self, page: Page, url: str, load_time: float):
        """Internal method that runs the actual KPI check logic"""
        kpi_name = self.kpi.get('kpi_name', '').lower()

        try:
            # Partial outage - uses HTTP HEAD (fast) instead of browser navigation
            if 'partial outage' in kpi_name:
                inner_links = page.query_selector_all('a[href^="/"], a[href*="' + url + '"]')
                total_links_found = len(inner_links)
                failed_links = 0
                checked_links = 0

                for link in inner_links[:5]:
                    try:
                        href = link.get_attribute('href')
                        if href and not href.startswith('#'):
                            checked_links += 1
                            full_url = href if href.startswith('http') else url.rstrip('/') + href
                            resp = requests.head(full_url, timeout=3, verify=False, allow_redirects=True)
                            if resp.status_code >= 400:
                                failed_links += 1
                    except:
                        failed_links += 1

                has_partial_outage = failed_links > 0
                return {
                    "flag": has_partial_outage,
                    "value": f"{failed_links}/{checked_links}",
                    "details": f"Found {total_links_found} internal links, scanned {checked_links}, {failed_links} failed" + (f" ({(failed_links/checked_links*100):.1f}% failure rate)" if checked_links > 0 else "")
                }

            # Slow page load
            elif 'slow page load' in kpi_name:
                slow_threshold = 5.0
                return {
                    "flag": load_time > slow_threshold,
                    "value": round(load_time, 2),
                    "details": f"Load time: {load_time:.2f}s ({'SLOW' if load_time > slow_threshold else 'OK'})"
                }

            # Heavy pages consuming excessive data
            elif 'heavy pages' in kpi_name or 'excessive data' in kpi_name:
                page_size_bytes = len(page.content())
                page_size_mb = page_size_bytes / (1024 * 1024)
                heavy_threshold = 5.0

                return {
                    "flag": page_size_mb > heavy_threshold,
                    "value": round(page_size_mb, 2),
                    "details": f"Page size: {page_size_mb:.2f} MB ({'HEAVY' if page_size_mb > heavy_threshold else 'OK'})"
                }

            # Browser security warning
            elif 'security warning' in kpi_name:
                security_warnings = []
                page.on('console', lambda msg: security_warnings.append(msg.text) if 'security' in msg.text.lower() else None)

                return {
                    "flag": len(security_warnings) > 0,
                    "value": len(security_warnings),
                    "details": f"Security warnings: {len(security_warnings)}"
                }

            # Mixed content warnings
            elif 'mixed content' in kpi_name:
                if url.startswith('https://'):
                    content = page.content()
                    http_pattern = re.compile(r'http://[^\s"\'>]+')
                    http_resources = http_pattern.findall(content)
                    unique_http_resources = list(set(http_resources))

                    resource_list = ', '.join(unique_http_resources[:5])
                    if len(unique_http_resources) > 5:
                        resource_list += f' ... and {len(unique_http_resources) - 5} more'

                    return {
                        "flag": len(unique_http_resources) > 0,
                        "value": len(unique_http_resources),
                        "details": f"HTTP resources on HTTPS page: {len(unique_http_resources)}" + (f" - [{resource_list}]" if unique_http_resources else "")
                    }
                else:
                    return {
                        "flag": False,
                        "value": 0,
                        "details": "Site uses HTTP (not applicable)"
                    }

            # Suspicious redirects
            elif 'suspicious redirects' in kpi_name or 'redirect' in kpi_name:
                final_url = page.url
                was_redirected = final_url != url

                return {
                    "flag": was_redirected,
                    "value": final_url if was_redirected else url,
                    "details": f"Redirected to: {final_url}" if was_redirected else "No redirect"
                }

            # Privacy policy availability
            elif 'privacy policy' in kpi_name:
                privacy_links = page.query_selector_all('a[href*="privacy"], a:has-text("Privacy Policy")')
                has_privacy_policy = len(privacy_links) > 0

                return {
                    "flag": not has_privacy_policy,
                    "value": len(privacy_links),
                    "details": f"Privacy policy link {'found' if has_privacy_policy else 'NOT FOUND'}"
                }

            # Page loads but assets don't (broken CSS/JS)
            elif 'assets' in kpi_name or 'broken css' in kpi_name:
                failed_resources = []
                resource_types = {}

                def track_failed(request):
                    failed_resources.append(request.url)
                    resource_type = request.resource_type
                    resource_types[resource_type] = resource_types.get(resource_type, 0) + 1

                page.on('requestfailed', track_failed)
                page.reload()
                time.sleep(1)

                has_broken_assets = len(failed_resources) > 0
                detail_msg = f"Failed resources: {len(failed_resources)}"
                if resource_types:
                    type_summary = ", ".join([f"{count} {rtype}" for rtype, count in list(resource_types.items())[:3]])
                    detail_msg += f" - Types: [{type_summary}]"

                return {
                    "flag": has_broken_assets,
                    "value": len(failed_resources),
                    "details": detail_msg
                }

            # Search not available
            elif 'search not available' in kpi_name or 'search' in kpi_name:
                selectors = [
                    'input[type="search"]',
                    'input[name*="search" i]',
                    'input[placeholder*="search" i]',
                    'input[id*="search" i]',
                    'input[class*="search" i]',
                    '[role="search"]',
                    'form[action*="search" i]',
                    '.search-form',
                    '.search-box',
                    '.searchbox',
                ]
                combined = ', '.join(selectors)
                search_elements = page.query_selector_all(combined)
                has_search = len(search_elements) > 0

                return {
                    "flag": not has_search,
                    "value": len(search_elements),
                    "details": f"Search functionality {'found' if has_search else 'NOT FOUND'} ({len(search_elements)} elements matched)"
                }

            # Broken internal links - uses HTTP HEAD (fast) instead of browser navigation
            elif 'broken internal links' in kpi_name or 'broken links' in kpi_name:
                all_links = page.query_selector_all('a[href]')
                internal_links = []

                for link in all_links:
                    href = link.get_attribute('href')
                    if href and (href.startswith('/') or url in href) and not href.startswith('#'):
                        internal_links.append(href)

                total_internal_links = len(internal_links)
                broken_count = 0
                checked_count = min(len(internal_links), 5)

                for link_url in internal_links[:5]:
                    try:
                        full_url = link_url if link_url.startswith('http') else url.rstrip('/') + link_url
                        resp = requests.head(full_url, timeout=3, verify=False, allow_redirects=True)
                        if resp.status_code >= 400:
                            broken_count += 1
                    except:
                        broken_count += 1

                broken_percentage = (broken_count / checked_count * 100) if checked_count > 0 else 0

                return {
                    "flag": broken_count > 0,
                    "value": round(broken_percentage, 2),
                    "details": f"Found {total_internal_links} internal links, checked {checked_count}, {broken_count} broken ({broken_percentage:.1f}%)"
                }

            # Circular navigation - reduced scope, faster timeouts
            elif 'circular navigation' in kpi_name:
                navigation_chain = [url]
                all_links = page.query_selector_all('a[href]')
                links = all_links[:3]
                links_tested = 0

                has_circular_nav = False
                for link in links:
                    try:
                        links_tested += 1
                        link.click(timeout=2000)
                        page.wait_for_load_state('load', timeout=3000)
                        current_url = page.url

                        if current_url in navigation_chain:
                            has_circular_nav = True
                            break

                        navigation_chain.append(current_url)
                        page.go_back()
                    except:
                        pass

                return {
                    "flag": has_circular_nav,
                    "value": len(navigation_chain),
                    "details": f"Tested {links_tested} links from {len(all_links)} total, chain depth: {len(navigation_chain)}, circular: {'YES' if has_circular_nav else 'NO'}"
                }

            # Download success rate & broken download links
            elif 'download' in kpi_name:
                download_links = page.query_selector_all('a[href$=".pdf"], a[href$=".doc"], a[href$=".docx"], a[href$=".xls"], a[href$=".xlsx"], a[download]')

                total_found = len(download_links)
                broken_downloads = 0
                total_downloads = min(len(download_links), 5)

                for link in download_links[:5]:
                    try:
                        href = link.get_attribute('href')
                        if href:
                            full_url = href if href.startswith('http') else url.rstrip('/') + href
                            response = requests.head(full_url, timeout=3, verify=False)
                            if response.status_code >= 400:
                                broken_downloads += 1
                    except:
                        broken_downloads += 1

                has_broken_downloads = broken_downloads > 0

                return {
                    "flag": has_broken_downloads,
                    "value": f"{broken_downloads}/{total_downloads}",
                    "details": f"Found {total_found} download links, checked {total_downloads}, {broken_downloads} broken" + (f" ({(broken_downloads/total_downloads*100):.1f}%)" if total_downloads > 0 else "")
                }

            # Default browser check
            else:
                return {
                    "flag": False,
                    "value": round(load_time, 2),
                    "details": "Page loaded successfully"
                }

        except Exception as e:
            return {
                "flag": True,
                "value": None,
                "details": str(e)
            }


class SharedBrowserContext:
    """
    Context manager for sharing a browser instance across multiple KPI checks.

    Usage:
        with SharedBrowserContext() as ctx:
            page, load_time = ctx.navigate_to(url)
            for kpi in browser_kpis:
                runner = BrowserKPIRunner(asset, kpi)
                result = runner.run_with_page(page, load_time)
    """

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
        )
        self.page = self.context.new_page()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def navigate_to(self, url: str, timeout: int = 30000):
        """
        Navigate to URL and return (page, load_time).
        Uses progressive fallback: load -> domcontentloaded -> commit
        Never raises - always returns a page (even if partially loaded).

        Args:
            url: URL to navigate to
            timeout: Navigation timeout in milliseconds

        Returns:
            tuple: (page, load_time_seconds, success_bool)
        """
        start = time.time()
        success = True

        try:
            # Try "load" first - fires when page fully loaded
            self.page.goto(url, timeout=timeout, wait_until='load')
        except Exception:
            success = False
            try:
                # Fallback to domcontentloaded - fires when HTML is parsed
                self.page.goto(url, timeout=timeout, wait_until='domcontentloaded')
                success = True
            except Exception:
                try:
                    # Last resort - just navigate without waiting
                    self.page.goto(url, timeout=timeout, wait_until='commit')
                    success = True
                except Exception:
                    # Even commit failed - page might still be usable
                    pass

        load_time = time.time() - start
        return self.page, load_time, success

    def reset_page(self):
        """Reset the page for a new asset (clear state, go to about:blank)"""
        self.page.goto('about:blank')
