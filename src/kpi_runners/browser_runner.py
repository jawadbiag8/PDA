from playwright.sync_api import sync_playwright
from src.kpi_runners.base import BaseKPIRunner
import time

class BrowserKPIRunner(BaseKPIRunner):

    def run(self):
        url = self.asset['url']
        kpi_name = self.kpi.get('kpi_name', '').lower()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )
            page = context.new_page()

            start = time.time()
            try:
                page.goto(url, timeout=15000, wait_until='networkidle')
                load_time = time.time() - start

                # Partial outage (homepage loads, inner pages fail)
                if 'partial outage' in kpi_name:
                    # Check if inner links work
                    inner_links = page.query_selector_all('a[href^="/"], a[href*="' + url + '"]')
                    total_links_found = len(inner_links)
                    failed_links = 0
                    checked_links = 0

                    for link in inner_links[:5]:  # Check first 5 internal links
                        try:
                            href = link.get_attribute('href')
                            if href and not href.startswith('#'):
                                checked_links += 1
                                response = page.goto(href if href.startswith('http') else url + href, timeout=5000)
                                if response.status >= 400:
                                    failed_links += 1
                                page.go_back()
                        except:
                            failed_links += 1

                    has_partial_outage = failed_links > 0
                    browser.close()
                    return {
                        "flag": has_partial_outage,  # True if inner pages fail
                        "value": f"{failed_links}/{checked_links}",
                        "details": f"Found {total_links_found} internal links, scanned {checked_links}, {failed_links} failed" + (f" ({(failed_links/checked_links*100):.1f}% failure rate)" if checked_links > 0 else "")
                    }

                # Slow page load
                elif 'slow page load' in kpi_name:
                    slow_threshold = 5.0  # Consider slow if > 5 seconds
                    browser.close()
                    return {
                        "flag": load_time > slow_threshold,  # True if slow
                        "value": round(load_time, 2),
                        "details": f"Load time: {load_time:.2f}s ({'SLOW' if load_time > slow_threshold else 'OK'})"
                    }

                # Heavy pages consuming excessive data
                elif 'heavy pages' in kpi_name or 'excessive data' in kpi_name:
                    # Get page size
                    page_size_bytes = len(page.content())
                    page_size_mb = page_size_bytes / (1024 * 1024)
                    heavy_threshold = 5.0  # Consider heavy if > 5 MB
                    
                    browser.close()
                    return {
                        "flag": page_size_mb > heavy_threshold,  # True if too heavy
                        "value": round(page_size_mb, 2),
                        "details": f"Page size: {page_size_mb:.2f} MB ({'HEAVY' if page_size_mb > heavy_threshold else 'OK'})"
                    }

                # Browser security warning
                elif 'security warning' in kpi_name:
                    # Check for security warnings (console errors with "security" keyword)
                    security_warnings = []
                    page.on('console', lambda msg: security_warnings.append(msg.text) if 'security' in msg.text.lower() else None)
                    
                    browser.close()
                    return {
                        "flag": len(security_warnings) > 0,  # True if warnings found
                        "value": len(security_warnings),
                        "details": f"Security warnings: {len(security_warnings)}"
                    }

                # Mixed content warnings
                elif 'mixed content' in kpi_name:
                    # Check for HTTP resources on HTTPS page
                    if url.startswith('https://'):
                        content = page.content()

                        # Find all HTTP resources (excluding https://)
                        import re
                        http_pattern = re.compile(r'http://[^\s"\'>]+')
                        http_resources = http_pattern.findall(content)

                        # Remove duplicates
                        unique_http_resources = list(set(http_resources))

                        resource_list = ', '.join(unique_http_resources[:5])  # Show first 5
                        if len(unique_http_resources) > 5:
                            resource_list += f' ... and {len(unique_http_resources) - 5} more'

                        browser.close()
                        return {
                            "flag": len(unique_http_resources) > 0,  # True if mixed content found
                            "value": len(unique_http_resources),
                            "details": f"HTTP resources on HTTPS page: {len(unique_http_resources)}" + (f" - [{resource_list}]" if unique_http_resources else "")
                        }
                    else:
                        browser.close()
                        return {
                            "flag": False,  # Not applicable for HTTP sites
                            "value": 0,
                            "details": "Site uses HTTP (not applicable)"
                        }

                # Suspicious redirects
                elif 'suspicious redirects' in kpi_name or 'redirect' in kpi_name:
                    final_url = page.url
                    was_redirected = final_url != url
                    
                    browser.close()
                    return {
                        "flag": was_redirected,  # True if redirected
                        "value": final_url if was_redirected else url,
                        "details": f"Redirected to: {final_url}" if was_redirected else "No redirect"
                    }

                # Privacy policy availability
                elif 'privacy policy' in kpi_name:
                    # Search for privacy policy link
                    privacy_links = page.query_selector_all('a[href*="privacy"], a:has-text("Privacy Policy")')
                    has_privacy_policy = len(privacy_links) > 0
                    
                    browser.close()
                    return {
                        "flag": not has_privacy_policy,  # True if NOT found (problem)
                        "value": len(privacy_links),
                        "details": f"Privacy policy link {'found' if has_privacy_policy else 'NOT FOUND'}"
                    }

                # Page loads but assets don't (broken CSS/JS)
                elif 'assets' in kpi_name or 'broken css' in kpi_name:
                    # Check for failed resource requests
                    failed_resources = []
                    resource_types = {}

                    def track_failed(request):
                        failed_resources.append(request.url)
                        resource_type = request.resource_type
                        resource_types[resource_type] = resource_types.get(resource_type, 0) + 1

                    page.on('requestfailed', track_failed)

                    page.reload()
                    time.sleep(2)  # Wait for resources to load

                    has_broken_assets = len(failed_resources) > 0

                    detail_msg = f"Failed resources: {len(failed_resources)}"
                    if resource_types:
                        type_summary = ", ".join([f"{count} {rtype}" for rtype, count in list(resource_types.items())[:3]])
                        detail_msg += f" - Types: [{type_summary}]"

                    browser.close()
                    return {
                        "flag": has_broken_assets,  # True if assets failed
                        "value": len(failed_resources),
                        "details": detail_msg
                    }

                # Search not available
                elif 'search not available' in kpi_name or 'search' in kpi_name:
                    # Look for search input/form using multiple selectors
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

                    browser.close()
                    return {
                        "flag": not has_search,  # True if search NOT found (problem)
                        "value": len(search_elements),
                        "details": f"Search functionality {'found' if has_search else 'NOT FOUND'} ({len(search_elements)} elements matched)"
                    }

                # Broken internal links
                elif 'broken internal links' in kpi_name or 'broken links' in kpi_name:
                    all_links = page.query_selector_all('a[href]')
                    internal_links = []

                    for link in all_links:
                        href = link.get_attribute('href')
                        if href and (href.startswith('/') or url in href) and not href.startswith('#'):
                            internal_links.append(href)

                    total_internal_links = len(internal_links)
                    broken_count = 0
                    checked_count = min(len(internal_links), 10)  # Check first 10 links

                    for link_url in internal_links[:10]:
                        try:
                            full_url = link_url if link_url.startswith('http') else url.rstrip('/') + link_url
                            response = page.goto(full_url, timeout=5000)
                            if response.status >= 400:
                                broken_count += 1
                            page.go_back()
                        except:
                            broken_count += 1

                    broken_percentage = (broken_count / checked_count * 100) if checked_count > 0 else 0

                    browser.close()
                    return {
                        "flag": broken_count > 0,  # True if any broken links found
                        "value": round(broken_percentage, 2),
                        "details": f"Found {total_internal_links} internal links, checked {checked_count}, {broken_count} broken ({broken_percentage:.1f}%)"
                    }

                # Circular navigation
                elif 'circular navigation' in kpi_name:
                    # Check if clicking links leads back to same page
                    navigation_chain = [url]
                    all_links = page.query_selector_all('a[href]')
                    links = all_links[:5]
                    links_tested = 0

                    has_circular_nav = False
                    for link in links:
                        try:
                            links_tested += 1
                            link.click(timeout=3000)
                            page.wait_for_load_state('networkidle', timeout=5000)
                            current_url = page.url

                            if current_url in navigation_chain:
                                has_circular_nav = True
                                break

                            navigation_chain.append(current_url)
                            page.go_back()
                        except:
                            pass

                    browser.close()
                    return {
                        "flag": has_circular_nav,  # True if circular navigation detected
                        "value": len(navigation_chain),
                        "details": f"Tested {links_tested} links from {len(all_links)} total, chain depth: {len(navigation_chain)}, circular: {'YES' if has_circular_nav else 'NO'}"
                    }

                # Download success rate & broken download links
                elif 'download' in kpi_name:
                    download_links = page.query_selector_all('a[href$=".pdf"], a[href$=".doc"], a[href$=".docx"], a[href$=".xls"], a[href$=".xlsx"], a[download]')

                    total_found = len(download_links)
                    broken_downloads = 0
                    total_downloads = min(len(download_links), 5)  # Check first 5

                    for link in download_links[:5]:
                        try:
                            href = link.get_attribute('href')
                            if href:
                                full_url = href if href.startswith('http') else url.rstrip('/') + href
                                import requests
                                response = requests.head(full_url, timeout=5, verify=False)
                                if response.status_code >= 400:
                                    broken_downloads += 1
                        except:
                            broken_downloads += 1

                    has_broken_downloads = broken_downloads > 0

                    browser.close()
                    return {
                        "flag": has_broken_downloads,  # True if broken downloads found
                        "value": f"{broken_downloads}/{total_downloads}",
                        "details": f"Found {total_found} download links, checked {total_downloads}, {broken_downloads} broken" + (f" ({(broken_downloads/total_downloads*100):.1f}%)" if total_downloads > 0 else "")
                    }

                # Default browser check
                else:
                    browser.close()
                    return {
                        "flag": False,  # No problem - page loaded
                        "value": round(load_time, 2),
                        "details": "Page loaded successfully"
                    }

            except Exception as e:
                browser.close()
                return {
                    "flag": True,  # Problem - page failed to load
                    "value": None,
                    "details": str(e)
                }