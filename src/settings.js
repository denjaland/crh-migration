export default {
  
  databases: {
    source: {
      name: 'ContractRegistrationService_mig_source',
      backup_file: 'F:\\Backup\\oltp\\ContractRegistrationService_anon_20210430.bak',
      files: {
        data: {
          logical: 'ContractRegistrationService',
          physical: 'F:\\data\\oltp\\ContractRegistrationService_mig_source.mdf'
        },
        log: {
          logical: 'ContractRegistrationService_log',
          physical: 'F:\\data\\oltp\\ContractRegistrationService_mig_source_log.mdf'
        }
      }
    },
    target: {
      name: 'UnifiedRegisters_mig_target',
      backup_file: 'F:\\Backup\\oltp\\UnifiedRegisters_anon_20210430.bak',
      files: {
        data: {
          logical: 'UnifiedRegisters',
          physical: 'F:\\data\\oltp\\UnifiedRegisters_mig_target.mdf'
        },
        log: {
          logical: 'UnifiedRegisters_log',
          physical: 'F:\\data\\oltp\\UnifiedRegisters_mig_target_log.mdf'
        }
      }
    },
    staging: {
      name: 'UnifiedRegisters_mig_staging',
      backup_file: 'F:\\Backup\\oltp\\UnifiedRegisters_anon_20210430.bak',
      files: {
        data: {
          logical: 'UnifiedRegisters',
          physical: 'F:\\data\\oltp\\UnifiedRegisters_mig_staging.mdf'
        },
        log: {
          logical: 'UnifiedRegisters_log',
          physical: 'F:\\data\\oltp\\UnifiedRegisters_mig_staging_log.mdf'
        }
      }
    }
  }

}