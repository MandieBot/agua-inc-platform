import logging
from typing import Dict, Any, Tuple

# Set up logging for internal corporate PMO tools
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AguaInc.PMO.SchedulingAPI")

class AllocationValidationError(Exception):
    """Exception raised when an assignment violates maximum allocation guardrails."""
    pass

class ProjectSchedulingMiddleware:
    def __init__(self):
        logger.info("Scheduling Validation Middleware successfully injected into API pipeline.")

    def _get_current_total_allocation(self, employee_id: str) -> float:
        """
        Simulates a database aggregation query looking across all active sprints 
        and parallel projects for this specific employee.
        """
        # Dummy data lookup: Mocking a senior engineer heavily assigned across multi-region projects
        mock_db_lookup = {
            "EMP-4402": 95.0,  # Near capacity (95%)
            "EMP-1289": 115.0, # Overloaded, will trigger soft warning (115%)
            "EMP-9931": 40.0    # Has bandwidth (40%)
        }
        return mock_db_lookup.get(employee_id, 0.0)

    def process_assignment_request(self, payload: Dict[str, Any]) -> Tuple[int, str]:
        """
        Intercepts incoming POST requests to /api/v1/project/assign.
        Validates resource constraints before committing to the database.
        """
        employee_id = payload.get("employee_id")
        project_id = payload.get("project_id")
        requested_allocation = payload.get("requested_allocation", 0.0)

        # 1. Fetch current workload bounds
        current_allocation = self._get_current_total_allocation(employee_id)
        projected_allocation = current_allocation + requested_allocation

        logger.info(f"[MIDDLEWARE CHECK] Evaluating assignment of {employee_id} to Project {project_id}.")
        logger.info(f"Current Workload: {current_allocation}% | Requested: {requested_allocation}% | Projected: {projected_allocation}%")

        # 2. Rule Enforcements
        # CRITICAL RULE: Hard block if capacity exceeds 120%
        if projected_allocation > 120.0:
            logger.error(f"❌ [409 CONFLICT] Allocation block! {employee_id} would reach {projected_allocation}%, exceeding the 120% hard limit.")
            return 409, f"Conflict: Employee allocation limit exceeded. Maximum allowed is 120%. Projected: {projected_allocation}%"

        # WARNING RULE: Soft warning flag if capacity passes 100%
        elif projected_allocation > 100.0:
            logger.warning(f"⚠️ [WARN] Over-allocation detected. {employee_id} will be running at {projected_allocation}% capacity across concurrent sprints.")
            # In a real API, this might inject a warning header or notify the PMO lead via webhook
            return 200, "Success (with warnings): Employee over-allocated, but within allowable crunch thresholds."

        # SAFE RULE: Standard pass-through
        else:
            logger.info(f"✅ [PASS] Assignment validated. {employee_id} remains under standard capacity thresholds.")
            return 200, "Success: Assignment approved."


# Dummy API Execution Simulation
if __name__ == "__main__":
    middleware = ProjectSchedulingMiddleware()

    # Scenario A: Safe assignment passing through cleanly
    print("\n--- Testing Scenario A (Safe Assignment) ---")
    request_a = {"employee_id": "EMP-9931", "project_id": "PROJ-8821", "requested_allocation": 25.0}
    status, msg = middleware.process_assignment_request(request_a)
    print(f"API Response Code: {status} | Message: {msg}")

    # Scenario B: Triggering a soft over-allocation warning (>100%)
    print("\n--- Testing Scenario B (Soft Warning Trigger) ---")
    request_b = {"employee_id": "EMP-4402", "project_id": "PROJ-1102", "requested_allocation": 15.0}
    status, msg = middleware.process_assignment_request(request_b)
    print(f"API Response Code: {status} | Message: {msg}")

    # Scenario C: Triggering a hard block and 409 Conflict (>120%)
    print("\n--- Testing Scenario C (Hard Block Trigger) ---")
    request_c = {"employee_id": "EMP-1289", "project_id": "PROJ-0043", "requested_allocation": 10.0}
    status, msg = middleware.process_assignment_request(request_c)
    print(f"API Response Code: {status} | Message: {msg}")
