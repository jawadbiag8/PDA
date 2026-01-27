from playwright.sync_api import sync_playwright
from src.kpi_runners.base import BaseKPIRunner

class AccessibilityKPIRunner(BaseKPIRunner):

    def run(self):
        url = self.asset['url']
        kpi_name = self.kpi.get('kpi_name', '').lower()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                page.goto(url, timeout=15000)

                # WCAG compliance score
                if 'wcag' in kpi_name or 'compliance' in kpi_name:
                    try:
                        # Inject axe-core from CDN
                        page.add_script_tag(url="https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.1/axe.min.js")

                        # Run axe-core accessibility scan
                        axe_results = page.evaluate("""
                            async () => {
                                const results = await axe.run();
                                return {
                                    violations: results.violations.length,
                                    passes: results.passes.length,
                                    incomplete: results.incomplete.length,
                                    violationDetails: results.violations.map(v => ({
                                        id: v.id,
                                        impact: v.impact,
                                        description: v.description,
                                        nodes: v.nodes.length
                                    }))
                                };
                            }
                        """)

                        violations = axe_results['violations']
                        passes = axe_results['passes']
                        total_checks = violations + passes

                        # Calculate compliance score (percentage of passed checks)
                        compliance_score = (passes / total_checks * 100) if total_checks > 0 else 0

                        # Consider it a problem if score is below 80%
                        has_issues = compliance_score < 80

                        # Get top 3 critical violations
                        critical_violations = [v for v in axe_results['violationDetails'] if v['impact'] in ['critical', 'serious']]
                        violation_summary = ", ".join([f"{v['id']} ({v['nodes']} elements)" for v in critical_violations[:3]])
                        if len(critical_violations) > 3:
                            violation_summary += f" ... and {len(critical_violations) - 3} more"

                        browser.close()
                        return {
                            "flag": has_issues,
                            "value": round(compliance_score, 1),
                            "details": f"WCAG compliance: {compliance_score:.1f}% ({violations} violations, {passes} passes)" + (f" - Critical: [{violation_summary}]" if critical_violations else "")
                        }

                    except Exception as e:
                        browser.close()
                        return {
                            "flag": True,
                            "value": 0,
                            "details": f"WCAG check failed: {str(e)}"
                        }

                # Missing form labels
                elif 'form label' in kpi_name:
                    inputs = page.query_selector_all('input')
                    selects = page.query_selector_all('select')
                    textareas = page.query_selector_all('textarea')
                    forms = inputs + selects + textareas

                    unlabeled = 0

                    for form_element in forms:
                        element_id = form_element.get_attribute('id')
                        has_label = page.query_selector(f'label[for="{element_id}"]') if element_id else None
                        if not has_label:
                            unlabeled += 1

                    total_forms = len(forms)
                    unlabeled_percentage = (unlabeled / total_forms * 100) if total_forms > 0 else 0

                    browser.close()
                    return {
                        "flag": unlabeled > 0,  # True if missing labels
                        "value": round(unlabeled_percentage, 2),
                        "details": f"Found {len(inputs)} inputs, {len(selects)} selects, {len(textareas)} textareas - {unlabeled}/{total_forms} missing labels ({unlabeled_percentage:.1f}%)"
                    }

                # Images missing alt text
                elif 'alt text' in kpi_name:
                    images = page.query_selector_all('img')
                    missing_alt = 0
                    empty_alt = 0

                    for img in images:
                        alt = img.get_attribute('alt')
                        if not alt:
                            missing_alt += 1
                        elif alt.strip() == '':
                            empty_alt += 1

                    total_images = len(images)
                    total_problematic = missing_alt + empty_alt
                    missing_percentage = (total_problematic / total_images * 100) if total_images > 0 else 0

                    browser.close()
                    return {
                        "flag": total_problematic > 0,  # True if missing alt text
                        "value": round(missing_percentage, 2),
                        "details": f"Found {total_images} images - {missing_alt} missing alt, {empty_alt} empty alt ({missing_percentage:.1f}% problematic)"
                    }

                # Poor color contrast
                elif 'color contrast' in kpi_name or 'contrast' in kpi_name:
                    try:
                        # Inject axe-core for contrast checking
                        page.add_script_tag(url="https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.1/axe.min.js")

                        # Run only color-contrast rule
                        contrast_results = page.evaluate("""
                            async () => {
                                const results = await axe.run({
                                    runOnly: ['color-contrast']
                                });
                                return {
                                    violations: results.violations.length > 0 ? results.violations[0].nodes.length : 0,
                                    details: results.violations.length > 0 ? results.violations[0].nodes.map(n => n.target).slice(0, 5) : []
                                };
                            }
                        """)

                        violations = contrast_results['violations']
                        has_contrast_issues = violations > 0

                        detail_text = f"{violations} elements with poor color contrast"
                        if contrast_results['details']:
                            selectors = ", ".join([str(d) for d in contrast_results['details'][:3]])
                            detail_text += f" - Examples: [{selectors}]"

                        browser.close()
                        return {
                            "flag": has_contrast_issues,
                            "value": violations,
                            "details": detail_text if violations > 0 else "No color contrast issues found"
                        }

                    except Exception as e:
                        browser.close()
                        return {
                            "flag": True,
                            "value": 0,
                            "details": f"Color contrast check failed: {str(e)}"
                        }

                # Default accessibility check
                else:
                    browser.close()
                    return {
                        "flag": False,
                        "value": None,
                        "details": "Accessibility check completed"
                    }

            except Exception as e:
                browser.close()
                return {
                    "flag": True,  # Problem - check failed
                    "value": None,
                    "details": str(e)
                }