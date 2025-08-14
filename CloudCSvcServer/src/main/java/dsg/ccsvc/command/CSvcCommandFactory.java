package dsg.ccsvc.command;

import java.util.function.Supplier;

/**
 * Factory class for creating instances of {@link CSvcCommand}.
 */
public class CSvcCommandFactory {

    /**
     * All Cli Commands.
     */
    public enum Command {
        CREATE(CreateCommand::new),
        DELETE(DeleteCommand::new),
        SET(SetCommand::new),
        GET(GetCommand::new),
        LS(LsCommand::new),
        ADD_WATCH(AddWatchCommand::new);
        // TODO:
        //        DELETE_ALL(DeleteAllCommand::new),
        //        GET_ACL(GetAclCommand::new),
        //        SET_ACL(SetAclCommand::new),
        //        STAT(StatCommand::new),
        //        SYNC(SyncCommand::new),
        //        SET_QUOTA(SetQuotaCommand::new),
        //        LIST_QUOTA(ListQuotaCommand::new),
        //        DEL_QUOTA(DelQuotaCommand::new),
        //        ADD_AUTH(AddAuthCommand::new),
        //        RECONFIG(ReconfigCommand::new),
        //        GET_CONFIG(GetConfigCommand::new),
        //        VERSION(VersionCommand::new),
        //        WHO_AM_I(WhoAmICommand::new);

        private Supplier<? extends CSvcCommand> instantiator;

        private CSvcCommand getInstance() {
            return instantiator.get();
        }

        Command(Supplier<? extends CSvcCommand> instantiator) {
            this.instantiator = instantiator;
        }
    }

    /**
     * Creates a new {@link CSvcCommand} instance.
     * @param command the {@link Command} to create a new instance of
     * @return the new {@code CliCommand} instance
     */
    public static CSvcCommand getInstance (Command command) {
        return command.getInstance();
    }
}
