def assign_tickets(agents, tickets):
    assigned = {}  # Dictionary to store ticket-to-agent assignments
    unassigned = []  # List to store unassigned tickets

    for ticket in tickets:
        assigned_agent = None

        for agent in agents:
            # Check if the agent meets the expertise requirement
            expertise_level = agent['expertise'].get(ticket['category'], 0)
            if ticket['priority'] == "High" and expertise_level < 3:
                continue  # Skip agents with insufficient expertise for high-priority tickets

            # Assign the first available agent that meets the criteria
            assigned_agent = agent
            break

        if assigned_agent:
            # Assign the ticket to the agent
            assigned[ticket['id']] = assigned_agent['id']
        else:
            # Add ticket to the unassigned list
            unassigned.append(ticket['id'])

    return {
        "assigned": assigned,
        "unassigned": unassigned
    }

# Example Data
agents = [
    {"id": "A1", "expertise": {"Technical": 4, "Billing": 2, "Support": 5}},
    {"id": "A2", "expertise": {"Technical": 2, "Billing": 5, "Support": 3}},
    {"id": "A3", "expertise": {"Technical": 1, "Billing": 4, "Support": 2}}
]

tickets = [
    {"id": "T1", "priority": "High", "category": "Technical"},
    {"id": "T2", "priority": "High", "category": "Billing"},
    {"id": "T3", "priority": "Medium", "category": "Support"},
    {"id": "T4", "priority": "High", "category": "Support"}
]

# Run the assignment system
result = assign_tickets(agents, tickets)

# Display the results
print("Assigned Tickets:", result["assigned"])
print("Unassigned Tickets:", result["unassigned"])