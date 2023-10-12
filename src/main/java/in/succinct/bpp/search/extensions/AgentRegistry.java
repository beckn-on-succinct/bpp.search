package in.succinct.bpp.search.extensions;

import com.venky.swf.plugins.background.core.agent.Agent;
import in.succinct.bpp.search.agent.IncrementalSearchProcessor;

public class AgentRegistry {
    static {
        Agent.instance().registerAgentSeederTaskBuilder(IncrementalSearchProcessor.AGENT_NAME,new IncrementalSearchProcessor());
    }
}
