"""
KPI Quarterly Calculator Module
Aggregates monthly KPI data into quarterly reports for quarterly reporting requirements

Author: Claude
Date: 2025-12-09
"""

import psycopg2
from psycopg2 import extras
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import calendar


class KPIQuarterlyCalculator:
    """
    Calculates quarterly KPI summaries from monthly KPI data
    Supports various aggregation methods based on KPI type
    """

    def __init__(self, pool):
        """
        Initialize with database connection pool

        Args:
            pool: Database connection pool
        """
        self.pool = pool

        # Define aggregation method for each KPI
        # Methods: 'average', 'sum', 'latest', 'min', 'max', 'custom'
        self.kpi_aggregation_methods = {
            # Safety KPIs - Sum incidents across quarter
            'FR1': 'sum',
            'Near Miss': 'sum',

            # Maintenance Performance - Average adherence/availability
            'TTR (Time to Repair) Adherence': 'average',
            'MTBF Mean Time Between Failure': 'average',
            'Technical Availability Adherence': 'average',
            'MRT (Mean Response Time)': 'average',

            # Work Orders - Use end-of-quarter snapshot
            'WO opened vs WO closed': 'latest',
            'WO Backlog': 'latest',
            'WO age profile': 'latest',

            # PM - Average adherence
            'Preventive Maintenance Adherence': 'average',

            # Analysis - Latest report
            'Top Breakdown': 'latest',

            # Process - Average score
            'Purchaser Monthly process Confirmation': 'average',

            # Customer Satisfaction - Average
            'Purchaser satisfaction': 'average',

            # Quality - Sum non-conformances, average closure rate
            'Non Conformances raised': 'sum',
            'Non Conformances closed': 'average',

            # Procurement - Average delivery time and satisfaction
            'Mean Time to Deliver a Quote': 'average',
            'Purchaser Satisfaction Survey': 'latest'  # Yearly, so latest
        }

    def get_quarter_info(self, year: int, quarter: int) -> Dict:
        """
        Get quarter date range and period strings

        Args:
            year: Year (e.g., 2025)
            quarter: Quarter number (1-4)

        Returns:
            Dictionary with quarter information
        """
        if quarter not in [1, 2, 3, 4]:
            raise ValueError("Quarter must be 1, 2, 3, or 4")

        # Calculate start and end months
        start_month = (quarter - 1) * 3 + 1
        end_month = start_month + 2

        # Generate period strings for each month
        periods = [f"{year}-{month:02d}" for month in range(start_month, end_month + 1)]

        # Quarter label
        quarter_label = f"{year}-Q{quarter}"

        # Date range
        start_date = f"{year}-{start_month:02d}-01"
        last_day = calendar.monthrange(year, end_month)[1]
        end_date = f"{year}-{end_month:02d}-{last_day}"

        return {
            'year': year,
            'quarter': quarter,
            'label': quarter_label,
            'start_month': start_month,
            'end_month': end_month,
            'periods': periods,
            'start_date': start_date,
            'end_date': end_date
        }

    def get_monthly_kpi_data(self, kpi_name: str, periods: List[str]) -> List[Dict]:
        """
        Retrieve monthly KPI results for specified periods

        Args:
            kpi_name: Name of the KPI
            periods: List of period strings (e.g., ['2025-01', '2025-02', '2025-03'])

        Returns:
            List of monthly KPI result dictionaries
        """
        conn = self.pool.get_connection()
        try:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            placeholders = ','.join(['%s'] * len(periods))
            query = f"""
                SELECT r.*, d.function_code, d.description, d.acceptance_criteria, d.frequency
                FROM kpi_results r
                JOIN kpi_definitions d ON r.kpi_name = d.kpi_name
                WHERE r.kpi_name = %s
                AND r.measurement_period IN ({placeholders})
                ORDER BY r.measurement_period
            """

            cursor.execute(query, [kpi_name] + periods)
            results = cursor.fetchall()
            cursor.close()
            return results

        finally:
            self.pool.return_connection(conn)

    def aggregate_kpi_quarterly(self, kpi_name: str, monthly_data: List[Dict],
                                aggregation_method: str) -> Dict:
        """
        Aggregate monthly KPI data into quarterly summary

        Args:
            kpi_name: Name of the KPI
            monthly_data: List of monthly KPI results
            aggregation_method: Method to use ('average', 'sum', 'latest', 'min', 'max')

        Returns:
            Aggregated quarterly KPI data
        """
        if not monthly_data:
            return {
                'value': None,
                'text': 'No data available for this quarter',
                'meets_criteria': None,
                'data_points': 0
            }

        # Extract numeric values (handle None values)
        values = [
            float(item['calculated_value'])
            for item in monthly_data
            if item.get('calculated_value') is not None
        ]

        result = {
            'data_points': len(monthly_data),
            'months_with_data': len(values),
            'meets_criteria': None
        }

        if not values:
            result['value'] = None
            result['text'] = f'Data entered for {len(monthly_data)} month(s) but no numeric values'
            return result

        # Perform aggregation based on method
        if aggregation_method == 'average':
            result['value'] = round(sum(values) / len(values), 2)
            result['text'] = f"Quarterly Average: {result['value']:.2f} (from {len(values)} months)"

        elif aggregation_method == 'sum':
            result['value'] = round(sum(values), 2)
            result['text'] = f"Quarterly Total: {result['value']:.2f} (from {len(values)} months)"

        elif aggregation_method == 'latest':
            # Use the most recent month's data
            latest = monthly_data[-1]
            result['value'] = float(latest['calculated_value']) if latest.get('calculated_value') else None
            result['text'] = latest.get('calculated_text', f"Latest: {result['value']}")
            result['meets_criteria'] = latest.get('meets_criteria')

        elif aggregation_method == 'min':
            result['value'] = round(min(values), 2)
            result['text'] = f"Quarterly Minimum: {result['value']:.2f}"

        elif aggregation_method == 'max':
            result['value'] = round(max(values), 2)
            result['text'] = f"Quarterly Maximum: {result['value']:.2f}"

        # Determine if quarterly target is met (for average/sum methods)
        if aggregation_method in ['average', 'sum', 'min', 'max']:
            # Check how many months met criteria
            months_passing = sum(1 for item in monthly_data if item.get('meets_criteria') is True)
            months_failing = sum(1 for item in monthly_data if item.get('meets_criteria') is False)

            # Quarter passes if at least 2 out of 3 months passed (majority rule)
            if months_passing + months_failing > 0:
                result['meets_criteria'] = months_passing >= 2
                result['text'] += f" | {months_passing}/{len(values)} months passed"

        return result

    def calculate_quarterly_kpi(self, kpi_name: str, year: int, quarter: int) -> Dict:
        """
        Calculate quarterly KPI for a specific KPI, year, and quarter

        Args:
            kpi_name: Name of the KPI
            year: Year (e.g., 2025)
            quarter: Quarter number (1-4)

        Returns:
            Dictionary with quarterly KPI calculation
        """
        # Get quarter information
        quarter_info = self.get_quarter_info(year, quarter)

        # Get monthly data
        monthly_data = self.get_monthly_kpi_data(kpi_name, quarter_info['periods'])

        # Get aggregation method
        aggregation_method = self.kpi_aggregation_methods.get(kpi_name, 'average')

        # Aggregate data
        quarterly_result = self.aggregate_kpi_quarterly(kpi_name, monthly_data, aggregation_method)

        # Add metadata
        quarterly_result.update({
            'kpi_name': kpi_name,
            'quarter_label': quarter_info['label'],
            'year': year,
            'quarter': quarter,
            'periods': quarter_info['periods'],
            'aggregation_method': aggregation_method,
            'calculation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        # Add function code and description if available
        if monthly_data:
            quarterly_result['function_code'] = monthly_data[0].get('function_code')
            quarterly_result['description'] = monthly_data[0].get('description')
            quarterly_result['acceptance_criteria'] = monthly_data[0].get('acceptance_criteria')

        return quarterly_result

    def calculate_all_quarterly_kpis(self, year: int, quarter: int) -> List[Dict]:
        """
        Calculate all quarterly KPIs for a given year and quarter

        Args:
            year: Year (e.g., 2025)
            quarter: Quarter number (1-4)

        Returns:
            List of quarterly KPI calculations
        """
        results = []

        # Get all KPI names
        all_kpis = list(self.kpi_aggregation_methods.keys())

        for kpi_name in all_kpis:
            try:
                result = self.calculate_quarterly_kpi(kpi_name, year, quarter)
                results.append(result)
            except Exception as e:
                print(f"Error calculating quarterly KPI '{kpi_name}': {str(e)}")
                results.append({
                    'kpi_name': kpi_name,
                    'quarter_label': f"{year}-Q{quarter}",
                    'error': str(e)
                })

        return results

    def save_quarterly_kpi_result(self, quarterly_result: Dict, calculated_by: str = 'system'):
        """
        Save quarterly KPI result to database
        Uses a special period format: YYYY-QN (e.g., '2025-Q1')

        Args:
            quarterly_result: Quarterly KPI result dictionary
            calculated_by: Username of who calculated it
        """
        conn = self.pool.get_connection()
        try:
            cursor = conn.cursor()

            # Use quarter label as measurement period (e.g., '2025-Q1')
            measurement_period = quarterly_result['quarter_label']

            cursor.execute("""
                INSERT INTO kpi_results
                (kpi_name, measurement_period, calculated_value, calculated_text,
                 target_value, meets_criteria, calculated_by, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (kpi_name, measurement_period)
                DO UPDATE SET
                    calculated_value = EXCLUDED.calculated_value,
                    calculated_text = EXCLUDED.calculated_text,
                    target_value = EXCLUDED.target_value,
                    meets_criteria = EXCLUDED.meets_criteria,
                    calculated_by = EXCLUDED.calculated_by,
                    notes = EXCLUDED.notes,
                    calculation_date = CURRENT_TIMESTAMP
            """, (
                quarterly_result['kpi_name'],
                measurement_period,
                quarterly_result.get('value'),
                quarterly_result.get('text'),
                None,  # Target value (could be calculated)
                quarterly_result.get('meets_criteria'),
                calculated_by,
                f"Quarterly aggregation ({quarterly_result['aggregation_method']}) " +
                f"from periods: {', '.join(quarterly_result['periods'])}. " +
                f"Calculated on {quarterly_result['calculation_date']}"
            ))

            conn.commit()
            cursor.close()

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.pool.return_connection(conn)

    def generate_quarterly_report(self, year: int, quarter: int, save_to_db: bool = False,
                                  calculated_by: str = 'system') -> Dict:
        """
        Generate a complete quarterly report for all KPIs

        Args:
            year: Year (e.g., 2025)
            quarter: Quarter number (1-4)
            save_to_db: Whether to save results to database
            calculated_by: Username of who generated the report

        Returns:
            Dictionary with quarterly report data
        """
        quarter_info = self.get_quarter_info(year, quarter)

        print(f"\n{'='*70}")
        print(f"Generating Quarterly KPI Report: {quarter_info['label']}")
        print(f"{'='*70}")
        print(f"Period: {quarter_info['start_date']} to {quarter_info['end_date']}")
        print(f"Months: {', '.join(quarter_info['periods'])}")
        print(f"{'='*70}\n")

        # Calculate all quarterly KPIs
        quarterly_kpis = self.calculate_all_quarterly_kpis(year, quarter)

        # Statistics
        total_kpis = len(quarterly_kpis)
        kpis_with_data = sum(1 for kpi in quarterly_kpis if kpi.get('value') is not None and 'error' not in kpi)
        kpis_passing = sum(1 for kpi in quarterly_kpis if kpi.get('meets_criteria') is True)
        kpis_failing = sum(1 for kpi in quarterly_kpis if kpi.get('meets_criteria') is False)
        kpis_pending = total_kpis - kpis_with_data

        # Save to database if requested
        if save_to_db:
            saved_count = 0
            for kpi_result in quarterly_kpis:
                if 'error' not in kpi_result and kpi_result.get('value') is not None:
                    try:
                        self.save_quarterly_kpi_result(kpi_result, calculated_by)
                        saved_count += 1
                    except Exception as e:
                        print(f"Error saving {kpi_result['kpi_name']}: {str(e)}")

            print(f"✓ Saved {saved_count} quarterly KPIs to database\n")

        # Print summary
        print(f"Summary:")
        print(f"  Total KPIs:        {total_kpis}")
        print(f"  With Data:         {kpis_with_data}")
        print(f"  Passing:           {kpis_passing}")
        print(f"  Failing:           {kpis_failing}")
        print(f"  Pending:           {kpis_pending}")
        print(f"{'='*70}\n")

        return {
            'quarter_info': quarter_info,
            'quarterly_kpis': quarterly_kpis,
            'statistics': {
                'total_kpis': total_kpis,
                'kpis_with_data': kpis_with_data,
                'kpis_passing': kpis_passing,
                'kpis_failing': kpis_failing,
                'kpis_pending': kpis_pending
            },
            'generated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'generated_by': calculated_by
        }

    def get_quarterly_kpi_results(self, year: int, quarter: int) -> List[Dict]:
        """
        Retrieve saved quarterly KPI results from database

        Args:
            year: Year
            quarter: Quarter number (1-4)

        Returns:
            List of quarterly KPI results
        """
        quarter_label = f"{year}-Q{quarter}"

        conn = self.pool.get_connection()
        try:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            cursor.execute("""
                SELECT r.*, d.function_code, d.description, d.acceptance_criteria, d.frequency
                FROM kpi_results r
                JOIN kpi_definitions d ON r.kpi_name = d.kpi_name
                WHERE r.measurement_period = %s
                ORDER BY d.function_code, r.kpi_name
            """, (quarter_label,))

            results = cursor.fetchall()
            cursor.close()
            return results

        finally:
            self.pool.return_connection(conn)

    def get_available_quarters(self) -> List[Dict]:
        """
        Get list of quarters that have KPI data available

        Returns:
            List of dictionaries with quarter information
        """
        conn = self.pool.get_connection()
        try:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            cursor.execute("""
                SELECT DISTINCT measurement_period
                FROM kpi_results
                WHERE measurement_period ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
                ORDER BY measurement_period DESC
            """)

            monthly_periods = cursor.fetchall()
            cursor.close()

            # Extract unique quarters
            quarters = set()
            for row in monthly_periods:
                period = row['measurement_period']
                year, month = map(int, period.split('-'))
                quarter = ((month - 1) // 3) + 1
                quarters.add((year, quarter))

            # Convert to list of dicts
            quarter_list = []
            for year, quarter in sorted(quarters, reverse=True):
                quarter_info = self.get_quarter_info(year, quarter)
                quarter_list.append({
                    'year': year,
                    'quarter': quarter,
                    'label': quarter_info['label'],
                    'display': f"Q{quarter} {year}"
                })

            return quarter_list

        finally:
            self.pool.return_connection(conn)

    def export_quarterly_summary_table(self, year: int, quarter: int) -> str:
        """
        Generate a text-based summary table for quarterly KPIs

        Args:
            year: Year
            quarter: Quarter number

        Returns:
            Formatted text table
        """
        quarterly_kpis = self.calculate_all_quarterly_kpis(year, quarter)
        quarter_label = f"{year}-Q{quarter}"

        lines = []
        lines.append(f"\n{'='*100}")
        lines.append(f"QUARTERLY KPI REPORT: {quarter_label}")
        lines.append(f"{'='*100}")
        lines.append(f"{'KPI Name':<40} {'Value':<20} {'Status':<15} {'Data Points':<10}")
        lines.append(f"{'-'*100}")

        for kpi in quarterly_kpis:
            kpi_name = kpi.get('kpi_name', 'Unknown')[:38]

            if 'error' in kpi:
                value_str = f"ERROR: {kpi['error'][:15]}"
                status_str = "ERROR"
                data_points = "0"
            elif kpi.get('value') is None:
                value_str = "No Data"
                status_str = "Pending"
                data_points = str(kpi.get('data_points', 0))
            else:
                value_str = f"{kpi['value']:.2f}"
                if kpi.get('meets_criteria') is True:
                    status_str = "✓ PASS"
                elif kpi.get('meets_criteria') is False:
                    status_str = "✗ FAIL"
                else:
                    status_str = "N/A"
                data_points = f"{kpi.get('months_with_data', 0)}/3"

            lines.append(f"{kpi_name:<40} {value_str:<20} {status_str:<15} {data_points:<10}")

        lines.append(f"{'='*100}\n")

        return '\n'.join(lines)


def demo_quarterly_calculator(pool):
    """
    Demonstration function showing how to use the Quarterly Calculator

    Args:
        pool: Database connection pool
    """
    calculator = KPIQuarterlyCalculator(pool)

    # Example 1: Calculate quarterly KPIs for Q4 2024
    print("\n" + "="*70)
    print("DEMO: Quarterly KPI Calculator")
    print("="*70)

    # Get available quarters
    print("\n1. Available quarters with data:")
    available_quarters = calculator.get_available_quarters()
    for q in available_quarters[:5]:  # Show first 5
        print(f"   - {q['display']} ({q['label']})")

    if available_quarters:
        # Use the most recent quarter
        recent = available_quarters[0]
        year = recent['year']
        quarter = recent['quarter']

        print(f"\n2. Calculating quarterly KPIs for {recent['display']}...")

        # Calculate a single KPI
        print("\n   Example: Preventive Maintenance Adherence")
        pm_result = calculator.calculate_quarterly_kpi('Preventive Maintenance Adherence', year, quarter)
        print(f"   - Value: {pm_result.get('value')}")
        print(f"   - Text: {pm_result.get('text')}")
        print(f"   - Status: {pm_result.get('meets_criteria')}")
        print(f"   - Method: {pm_result.get('aggregation_method')}")

        # Generate full report
        print(f"\n3. Generating full quarterly report...")
        report = calculator.generate_quarterly_report(year, quarter, save_to_db=False)

        # Show summary table
        print(calculator.export_quarterly_summary_table(year, quarter))
    else:
        print("\n   No monthly KPI data available yet. Please enter monthly data first.")

    print("="*70)
    print("Demo complete!")
    print("="*70 + "\n")


if __name__ == "__main__":
    print("KPI Quarterly Calculator Module")
    print("Import this module and use KPIQuarterlyCalculator class")
    print("\nExample usage:")
    print("  from kpi_quarterly_calculator import KPIQuarterlyCalculator")
    print("  calculator = KPIQuarterlyCalculator(db_pool)")
    print("  report = calculator.generate_quarterly_report(2025, 1, save_to_db=True)")
