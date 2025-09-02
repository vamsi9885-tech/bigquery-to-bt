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
