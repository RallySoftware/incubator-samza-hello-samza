package samza.examples.flowdock;

public class FlowdockKey {
    private String organization;
    private String flow;
    private String thread;
    private String type;
    private String username;

    public FlowdockKey() {
        type = "message";
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(final String organization) {
        this.organization = organization;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(final String flow) {
        this.flow = flow;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(final String thread) {
        this.thread = thread;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(final String username) {
        this.username = username;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }
}
