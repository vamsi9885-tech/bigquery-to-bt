1. Update FileDto.java
@NotNull(message = "partCount must not be null")
@Pattern(regexp = "\\*|[1-9][0-9]*", message = "partCount must be '*' or a positive integer")
private String partCount;   // change from Integer to String


This allows either:

"*"

"1", "2", "3", etc.


  2. Update FileConfig.java
@JsonProperty("part_count")
private String partCount;  // change from Integer to String


3. Update FileServiceImpl.java

Everywhere you check dto.getPartCount() < 1 you must handle string:

String pc = dto.getPartCount();
if (pc == null || (!pc.equals("*") && Integer.parseInt(pc) < 1)) {
    dto.setPartCount("1");
}


And same for update:

String pc = file.getPartCount();
if (pc == null || (!pc.equals("*") && Integer.parseInt(pc) < 1)) {
    file.setPartCount("1");
}






<TextInput
  label={FILE_FORM.LABELS.PART_COUNT}
  model="partCount"
  validators={{
    required: FILE_FORM.VALIDATION.PartCount_Required,
    validate: {
      validPartCount: (v: string) => {
        if (v === "*") return true;  // ✅ accept asterisk
        const num = Number(v);
        return (
          !isNaN(num) && num >= 1 || FILE_FORM.VALIDATION.PartCount_Positive
        );
      },
    },
  }}
  helper={FILE_FORM.HELPERS.PART_COUNT}
  data-testid="partcount-input"
  isDisabled={disableEdit}
/>



in filedto.java

   @NotNull(message = "partCount must not be null")
    @Min(value = 1, message = "partCount must be at least 1")
    private Integer partCount = 1;

in file config.java


    @JsonProperty("part_count")
    private Integer partCount;

in fileServieImpl.java

  @Override
    @Transactional
    public void saveFiles(String clientId, String feedIdentifier, List<FileDto> files) {
        String sanitizedClientId = Utils.sanitizeString(clientId);
        String sanitizedFeedIdentifier = Utils.sanitizeString(feedIdentifier);
        log.info("Saving files for clientId={}, feedIdentifier={}", sanitizedClientId, sanitizedFeedIdentifier);
        try{
        Feeds feed = getFeedOrThrow(feedIdentifier, clientId);
        if (!feed.isActive()) {
            throw new BadRequestException("Cannot save files: Feed is not active. Feed name:" + sanitizedFeedIdentifier);
        }
        if (files == null || files.isEmpty()) {
            log.warn("No files provided for feed: {}", sanitizedFeedIdentifier);
            throw new BadRequestException("No files provided for feed: " + feedIdentifier);
        }
        // Delegate all validation to validator
        fileConfigValidator.validateCreateRequest(files, feed);
        log.info("Successfully validated file creation request for feed: {} ", sanitizedFeedIdentifier);
        for (FileDto dto : files) {
            if (dto.getPartCount() == null || dto.getPartCount() < 1) {
                dto.setPartCount(1);
            }
            if (dto.getPartStartSeq() == null || dto.getPartStartSeq() < 1) {
                dto.setPartStartSeq(1);
            }
            String loggedInUser = Utils.getLoggedInUsername();
            Files entity = fileTransformer.toEntity(dto, feed.getFeedIdentifier(), loggedInUser);
            log.info("Successfully created file entity for feed: {} and logicalFile : {}", sanitizedFeedIdentifier, Utils.sanitizeString(dto.getLogicalFileName()));
            auditService.logAudit( Constant.AuditType.FILE_CREATION,FILE_LABLE, entity, null, clientId, UUID.fromString(feedIdentifier));
            fileRepository.save(entity);
        }
        log.info("Successfully saved files for feed: {}.", sanitizedFeedIdentifier);
        }
        catch (RecordAlreadyExistException e) {
            log.error("File already exists for feed: {} and client {}, error: {}", sanitizedFeedIdentifier, sanitizedClientId, e);
            throw new RecordAlreadyExistException("Given Logical File name already exists for feed: " + feedIdentifier);
        } catch (BadRequestException e) {
            log.error("Bad request while saving files for feed: {} and client {}, error: {}", sanitizedFeedIdentifier, sanitizedClientId, e);
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error while saving files for feed: {} and clientid: {} , error {}", sanitizedFeedIdentifier, sanitizedClientId, e);
            throw new UnExpectedException("An unexpected error occurred while saving files for feed: " + sanitizedFeedIdentifier, e);
        }
    }

    @Override
    @Transactional
    public void updateFile(String clientId, String feedIdentifier, String logicalFileName, FileDto file) {
        String sanitizedClientId = Utils.sanitizeString(clientId);
        String sanitizedFeedIdentifier = Utils.sanitizeString(feedIdentifier);
        String sanitizedLogicalFileName = Utils.sanitizeString(logicalFileName);
        log.info("Updating file for clientId={}, feedIdentifier={}, logicalFileName={}", 
                sanitizedClientId, 
                sanitizedFeedIdentifier, 
                sanitizedLogicalFileName);
        try{
        Feeds feed = getFeedOrThrow(feedIdentifier, clientId);
        if (!feed.isActive()) {
            throw new BadRequestException("Cannot update file: Feed is not active. Feed name:" + feedIdentifier);
        }
        Files fileDAO = fileRepository.findByFeedIdentifierAndLogicalFileName(feed.getFeedIdentifier(), logicalFileName)
                .orElseThrow(() -> new RecordNotFoundException(FILE_NOT_FOUND + logicalFileName));
        // Delegate all validation to validator
        Files oldEntity = deepCopyUtil.deepCopy(fileDAO, Files.class);
        fileConfigValidator.validateUpdateRequest(file, feed);
        log.info("Successfully validated file update request for feed: {} and logical file name: {}", sanitizedFeedIdentifier, sanitizedLogicalFileName);
        if (file.getPartCount() == null || file.getPartCount() < 1) {
            file.setPartCount(1);
        }
        if (file.getPartStartSeq() == null || file.getPartStartSeq() < 1) {
            file.setPartStartSeq(1);
        }
        String loggedInUser = Utils.getLoggedInUsername();
        Files updated = fileTransformer.updateToEntity(fileDAO, file, feed.getFeedIdentifier(), loggedInUser);
        fileRepository.save(updated);
        log.info("Successfully updated file entity for feed: {} and logical file name: {}", sanitizedFeedIdentifier, sanitizedLogicalFileName);
        auditService.logAudit( Constant.AuditType.FILE_UPDATE,FILE_LABLE, updated, oldEntity, clientId, UUID.fromString(feedIdentifier));
    }
    catch (RecordNotFoundException e) {
            log.error("File not found for feed: {} and logical file name: {}", sanitizedFeedIdentifier, sanitizedLogicalFileName, e);
            throw new RecordNotFoundException(FILE_NOT_FOUND + logicalFileName);
        } catch (BadRequestException | RecordAlreadyExistException e) {
            // RecordAlreadyExistException is caught here to handle cases where the logical file name already exists
            log.error("Bad request  or Record already exist while updating file for feed: {} and logical file name: {}", sanitizedFeedIdentifier, sanitizedLogicalFileName, e);
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error while updating file for feed: {} and logical file name: {}", sanitizedFeedIdentifier, sanitizedLogicalFileName, e);
            throw new UnExpectedException("An unexpected error occurred while updating file for feed: " + sanitizedFeedIdentifier, e);
        }
    }



FileTransformer.java

package com.optum.dap.api.transformer;

import java.io.File;

import com.optum.dap.api.dto.FileDto;
import com.optum.dap.api.model.Feeds;
import com.optum.dap.api.model.Files;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

import com.optum.dap.api.model.FileConfig;

/**
 * Transformer for FileConfig DTOs and Entity.
 * Handles mapping between DTOs and Files, including JSONB serialization.
 */
@Component
public class FileTransformer {

    /**
     * Converts Files to FileDto.
     * @param entity Files
     * @return FileDto
     */
    public FileDto toDto(Files entity) {
        if (entity == null) return null;
        FileDto dto = new FileDto();
        dto.setLogicalFileName(entity.getLogicalFileName());
        FileConfig fileConfig = entity.getFileConfig();
        dto.setFileId(fileConfig.getFileId());
        dto.setOrder(fileConfig.getOrder());
        dto.setFileNameFormat(fileConfig.getFileNameFormat());
        dto.setPartCount(fileConfig.getPartCount());
        dto.setPartStartSeq(fileConfig.getPartStartSeq());
        dto.setParameters(CommonTransformer.mapParametersToParameterDtos(fileConfig.getParameters()));
        dto.setFilter(fileConfig.getFilter());
        dto.setIsMandatory(fileConfig.getIsMandatory());

         // sets both transient and json fields
        return dto;
    }

    /**
     * Converts FileDto to Files.
     * @param dto FileDto
     * @param feedIdentifier UUID of the feed
     * @param user user performing the operation
     * @return Files
     */
    public Files toEntity(FileDto dto, UUID feedIdentifier, String user) {
        if (dto == null) return null;
        Files entity = new Files();
        entity.setFeedIdentifier(feedIdentifier);
        entity.setLogicalFileName(dto.getLogicalFileName());
        entity.setFileConfig(toFileConfig(dto , new FileConfig())); // sets both transient and json fields
        entity.setActive(dto.getIsMandatory() != null ? dto.getIsMandatory() : true);
        entity.setCreatedBy(user);
        entity.setCreatedDate(LocalDateTime.now());
        entity.setModifiedBy(user);
        entity.setModifiedDate(LocalDateTime.now());
        return entity;
    }

     /**
     * Converts FileDto to Files.
     * @param dto FileDto
     * @param feedIdentifier UUID of the feed
     * @param user user performing the operation
     * @return Files
     */
    public Files updateToEntity(Files file,FileDto dto, UUID feedIdentifier, String user) {
        if (dto == null || file == null) return null;
        file.setFileConfig(toFileConfig(dto, file.getFileConfig())); // sets both transient and json fields
        file.setActive(dto.getIsMandatory() != null ? dto.getIsMandatory() : true);
        file.setModifiedBy(user);
        file.setModifiedDate(LocalDateTime.now());
        return file; // changed from entity to file
    }
    /**
     * Converts FileDto to Files with Feeds reference.
     * @param dto FileDto
     * @param feed Feeds
     *     * @param user user performing the operation
     * @return Files
     */
    public Files toEntity(FileDto dto, Feeds feed, String user) {
        if (dto == null || feed == null) return null;
        Files entity = toEntity(dto, feed.getFeedIdentifier(), user);
        entity.setFeed(feed);
        return entity;
    }

    public FileConfig toFileConfig(FileDto dto,FileConfig fileConfig ) {
        if (dto == null) return null;
        fileConfig.setFileId(dto.getFileId());
        fileConfig.setOrder(dto.getOrder());
        fileConfig.setFileNameFormat(dto.getFileNameFormat());
        fileConfig.setPartCount(dto.getPartCount());
        fileConfig.setPartStartSeq(dto.getPartStartSeq());
        fileConfig.setParameters(CommonTransformer.convertDtoListToEntityList(dto.getParameters()));
        fileConfig.setFilter(dto.getFilter());
        fileConfig.setIsMandatory(dto.getIsMandatory());
        return fileConfig;
    }

}






"use client";
import React, {  useEffect, useState } from 'react';
import { FormProvider } from "@abyss/web/ui/FormProvider";
import { useForm } from "@abyss/web/hooks/useForm";
import {Button} from '@abyss/web/ui/Button';
import { Grid } from '@abyss/web/ui/Grid';
import { TextInput } from '@abyss/web/ui/TextInput';
import { Modal } from '@abyss/web/ui/Modal';
import { Layout } from '@abyss/web/ui/Layout';
import { Tooltip } from '@abyss/web/ui/Tooltip';
import { ToggleSwitch } from '@abyss/web/ui/ToggleSwitch';
import { useUpdateFile } from '@/src/api/clients/useClients';
import { INotification } from '@/src/types/common';
import  Notification  from '@/src/components/common/Notification';
import { FEED_TYPE, FILE_FORM, FEED_FORM } from "@/src/constants/common";
import {IEditFilesProps, IFile} from "@/src/types/common";
import { Flex } from "@abyss/web/ui/Flex";

const EditFiles: React.FC<IEditFilesProps> = ({ clientId, feedName, feedType, feedId, file, onClose, onSave, disableEdit}) => {
 const [parameters, setParameters] = useState(file?.parameters || undefined);
 const [parametersRemoved, setParametersRemoved] = useState(false);
 const [notification, setNotification] = React.useState<INotification>({
    type: undefined,
    message: "",
});
const defaultValues = {
  fileId: undefined,
  fileNameFormat: '',
  order: undefined,
  logicalFileName: '',
  isMandatory: true,
  filter: undefined,
  parameters: undefined,
  partCount: 1,
  partStartSeq: 1,
};
  const form = useForm<IFile>({
    defaultValues:  {...defaultValues}
  });

  useEffect(() => {
    if(!file) return;
    const mapperField:IFile = {...defaultValues};
    mapperField.fileId = file?.fileId;
    mapperField.fileNameFormat = file?.fileNameFormat;
    mapperField.logicalFileName= file?.logicalFileName;
    mapperField.isMandatory = file?.isMandatory || true;
    mapperField.filter = file?.filter || undefined;
    mapperField.parameters = file?.parameters;
    mapperField.order = file?.order;
    mapperField.partCount = file?.partCount || 1;
    mapperField.partStartSeq= file?.partStartSeq || 1;
    form.reset(mapperField);

  }, [file]);
  const editMode = file !== null;
  const { mutate: saveFile } = useUpdateFile();
  const addParam = () => {
    const currentParams = form.getValues('parameters') || [];
    const updatedParams = [...currentParams, { paramName: '', paramValue: '' }];
    setParameters(updatedParams);
    form.setValue('parameters', updatedParams);
  };

  const removeParam = (index: number) => {
    const currentParams = form.getValues('parameters') || [];
    const updatedParams = currentParams.filter((_: any, i: number) => i !== index);
    setParameters(updatedParams);
    form.setValue('parameters', updatedParams);
    setParametersRemoved(true);
  };
    const onCloseNotification = () => {
        setNotification({ type: undefined, message: "" });
    };

  const validateParameters = () => {
    const currentParams = form.getValues('parameters') || [];
    let isValid = true;

    currentParams.forEach((param: any, index: number) => {
      if (!param.paramName || !param.paramValue) {
        form.setError(`parameters.${index}.paramName`, {
          type: 'manual',
          message: 'Param Name and Value are required.',
        });
        isValid = false;
      }
    });

    return isValid;
  };

  const onSubmit = (data: any) => {
    const { isValid: formIsValid } = form.formState;
    const paramsAreValid = validateParameters();
    if (formIsValid && paramsAreValid && data) {
      const formDate = file === null? [data] : data;
      saveFile({ clientId, feedId, logicalFileName: file?.logicalFileName, data: formDate, method: editMode ? 'PUT' : 'POST' }, {
        onSuccess: () => {
          setNotification({
            type: 'success',
            message: FILE_FORM.MESSAGES.SAVE_SUCCESS,
          });
          setParametersRemoved(false);
          form.reset(data);
          onSave();
          setTimeout(() => {
            onClose();
          }, 1000);
        },
        onError: (error) => {
          setNotification({
            type: 'error',
            message: FILE_FORM.MESSAGES.SAVE_ERROR + error.message,
          });
        },
      });
    }
  };
  const { isValid,dirtyFields,isSubmitting } = form.formState;
  const enableSubmit = (isValid &&  Object.keys(dirtyFields).length > 0) || parametersRemoved || isSubmitting;
  return (
    <Modal
      title={file ? FILE_FORM.LABELS.MODAL_TITLE : FILE_FORM.LABELS.MODAL_CREATE_FILE}
      isOpen={true}
      onClose={onClose}
      closeOnClickOutside = {false}
    >
     <Notification type={notification.type} message={notification.message} onClose={onCloseNotification}  />
      <FormProvider state={form} onSubmit={onSubmit}>
        <Modal.Section>
           {disableEdit && <p style={{color: "red"}}>{FEED_FORM.MESSAGES.DISABLE_FEED_EDIT_MESSAGE}</p>}
          <Grid>
          <Grid.Col span={{ xs: 12, md: 12 }}>
              <TextInput
                label={FILE_FORM.LABELS.FEED_NAME}
                value={feedName}
                data-testid="feed-name-input"
                isDisabled={true}
              />
            </Grid.Col>
            <Grid.Col span={{ xs: 12, md: 12 }}>
              <TextInput
                label={FILE_FORM.LABELS.LOGICAL_FILE_NAME}
                model="logicalFileName"
                isDisabled={editMode || disableEdit}
                validators={{ required: FILE_FORM.VALIDATION.LogicalFileName_Required }}
                data-testid="logical-file-name-input"
                helper={FILE_FORM.HELPERS.LOGICAL_FILE_NAME}
              />
            </Grid.Col>
          { feedType === FEED_TYPE.Pull && (<><Grid.Col span={{ xs: 12, md: 3 }}>
              <TextInput
                label={FILE_FORM.LABELS.ORDER}
                model="order"
                mask="numeric"
                maskConfig={{
                  allowNegative: false,
                  decimalScale: 0,
                  value:  form.getValues("order")
                }}
                data-testid="order-input"
                helper={FILE_FORM.HELPERS.ORDER}
                isDisabled={disableEdit}
              />
            </Grid.Col>
            <Grid.Col span={{ xs: 12, md: 9 }}>
              <TextInput
                label={FILE_FORM.LABELS.QUERY_SCRIPT_FILE}
                model="fileId"
                data-testid="file-id-input"
                validators={{ required: FILE_FORM.VALIDATION.QueryScriptFile_Required }}
                helper={FILE_FORM.HELPERS.QUERY_SCRIPT_FILE}
                isDisabled={disableEdit}
              />
            </Grid.Col></>)}
          
            <Grid.Col span={{ xs: 12, md: 12 }}>
              <TextInput
                label={FILE_FORM.LABELS.FILE_NAME_FORMAT}
                model="fileNameFormat"
                validators={{ required: FILE_FORM.VALIDATION.FileNameFormat_Required}}
                data-testid="file-name-format-input"
                helper={FILE_FORM.HELPERS.FILE_NAME_FORMAT}
                isDisabled={disableEdit}
              />
            </Grid.Col>
            <Grid.Col span={{ xs: 12, md: 12}}>
                  <ToggleSwitch 
                                css={{
                                  "abyss-toggle-switch-root": {
                                    flexDirection: "row-reverse",
                                    justifyContent: "start",
                                    gap: "0.5rem",
                                  },
                                  "abyss-toggle-switch-label":{
                                    fontWeight: "bold",
                                  }
                                }}
                                 isDisabled
                                model="isMandatory"
                                label={FILE_FORM.LABELS.IS_FILE_MANDATORY}
                                data-testid="is-mandatory-toggle"
                />
            </Grid.Col>
            {feedType !== FEED_TYPE.Pull && (<>
              <Grid.Col span={{ xs: 12, md: 6 }}>
              <TextInput
                label={FILE_FORM.LABELS.PART_COUNT}
                model="partCount"
                validators={{ required: FILE_FORM.VALIDATION.PartCount_Required , validate: {
                  positive: (v: number) =>
                    v >= 1 || FILE_FORM.VALIDATION.PartCount_Positive,
                }}}
                 mask='numeric'
                 maskConfig={{
                  allowNegative: false,
                  decimalScale: 0,
                  value:  form.getValues("partCount")
                }}
                helper={FILE_FORM.HELPERS.PART_COUNT}
                data-testid="partcount-input"
                isDisabled={disableEdit}
              />
            </Grid.Col>
            <Grid.Col span={{ xs: 12, md: 6 }}>
              <TextInput
                label={FILE_FORM.LABELS.PART_START_SEQ}
                model="partStartSeq"
                mask='numeric'
                maskConfig={{
                  allowNegative: false,
                  decimalScale: 0,
                  value:  form.getValues("partStartSeq")
                }}
                helper={FILE_FORM.HELPERS.PART_START_SEQ}
                data-testid="part-start-seq-input"
                validators={{ required: FILE_FORM.VALIDATION.PartStartSeq_Positive , validate: {
                  positive: (v: number) =>
                    v >= 1 || FILE_FORM.VALIDATION.PartStartSeq_Positive,
                }}}
                isDisabled={disableEdit}
              />
            </Grid.Col>
            </>)}
            {feedType === FEED_TYPE.Pull && (<>
            <Grid.Col span={{ xs: 12, md: 12 }}>
              <TextInput
                label={FILE_FORM.LABELS.FILTERS}
                model="filter"
                placeholder={FILE_FORM.PLACEHOLDERS.FILTER}
                data-testid="filter-input"
                isDisabled={disableEdit}
              />
            </Grid.Col>
            {parameters?.map((param: any, index: number) => (
              <React.Fragment key={`param-name-input-${param.paramName}-${index}`}>
                <Grid.Col span={{ xs: 12, md: 5 }}>
                  <TextInput
                    label={FILE_FORM.LABELS.PARAM_NAME}
                    model={`parameters[${index}].paramName`}
                    data-testid={`param-name-input-${index}`}
                    placeholder={FILE_FORM.PLACEHOLDERS.PARAM_NAME + (index+1)}
                    isDisabled={disableEdit}
                  />
                </Grid.Col>
                <Grid.Col span={{ xs: 12, md: 5 }}>
                  <TextInput
                    label={FILE_FORM.LABELS.PARAM_VALUE}
                    model={`parameters[${index}].paramValue`}
                    data-testid={`param-value-input-${index}`}
                    placeholder={FILE_FORM.PLACEHOLDERS.PARAM_VALUE + (index+1)}
                    isDisabled={disableEdit}
                  />
                </Grid.Col>
                <Grid.Col span={{ xs: 12, md: 2 }}>
                <Flex alignItems="center" justify="center" style={{ paddingTop: "25px"}}> 
                  <Button variant="destructive" onClick={() => removeParam(index)}    isDisabled={disableEdit} >
                    {FILE_FORM.BUTTON_TEXT.REMOVE_PARAM}
                  </Button>
                  </Flex>
                </Grid.Col>
              </React.Fragment>
            ))}
            <Grid.Col span={{ xs: 12, md: 12 }} >
         
              <Button onClick={addParam} isDisabled={disableEdit} >{FILE_FORM.BUTTON_TEXT.ADD_PARAM}</Button>

            </Grid.Col>
          </>)}
          </Grid>
        </Modal.Section>
        <Modal.Section>
          <Layout.Group alignLayout="right">
            <Button variant="outline" onClick={onClose} >
              {FILE_FORM.BUTTON_TEXT.CANCEL}
            </Button>
            <Tooltip content={FILE_FORM.TOOLTIP.SAVE_BUTTON} >
              <Button
              
                onClick={() => {
                  form.handleSubmit(onSubmit)();
                }}
                isDisabled={!enableSubmit || disableEdit}
              >
                {FILE_FORM.BUTTON_TEXT.SAVE}
              </Button>
            </Tooltip>
          </Layout.Group>
        </Modal.Section>
      </FormProvider>
    </Modal>
  );
};

export default EditFiles;
