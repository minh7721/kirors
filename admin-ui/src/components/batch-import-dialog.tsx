import { useState } from 'react'
import { toast } from 'sonner'
import { CheckCircle2, XCircle, AlertCircle, Loader2 } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useCredentials, useAddCredential, useDeleteCredential } from '@/hooks/use-credentials'
import { getCredentialBalance, setCredentialDisabled } from '@/api/credentials'
import { extractErrorMessage, sha256Hex } from '@/lib/utils'
import { useLocale } from '@/lib/locale'

interface BatchImportDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

interface CredentialInput {
  refreshToken: string
  clientId?: string
  clientSecret?: string
  profileArn?: string
  region?: string
  authRegion?: string
  apiRegion?: string
  priority?: number
  machineId?: string
}

interface VerificationResult {
  index: number
  status: 'pending' | 'checking' | 'verifying' | 'verified' | 'duplicate' | 'failed'
  error?: string
  usage?: string
  email?: string
  credentialId?: number
  rollbackStatus?: 'success' | 'failed' | 'skipped'
  rollbackError?: string
}



export function BatchImportDialog({ open, onOpenChange }: BatchImportDialogProps) {
  const { t } = useLocale()
  const [jsonInput, setJsonInput] = useState('')
  const [importing, setImporting] = useState(false)
  const [progress, setProgress] = useState({ current: 0, total: 0 })
  const [currentProcessing, setCurrentProcessing] = useState<string>('')
  const [results, setResults] = useState<VerificationResult[]>([])

  const { data: existingCredentials } = useCredentials()
  const { mutateAsync: addCredential } = useAddCredential()
  const { mutateAsync: deleteCredential } = useDeleteCredential()

  const rollbackCredential = async (id: number): Promise<{ success: boolean; error?: string }> => {
    try {
      await setCredentialDisabled(id, true)
    } catch (error) {
      return {
        success: false,
        error: t('rollbackDisableFailed', { message: extractErrorMessage(error) }),
      }
    }

    try {
      await deleteCredential(id)
      return { success: true }
    } catch (error) {
      return {
        success: false,
        error: t('rollbackDeleteFailed', { message: extractErrorMessage(error) }),
      }
    }
  }

  const resetForm = () => {
    setJsonInput('')
    setProgress({ current: 0, total: 0 })
    setCurrentProcessing('')
    setResults([])
  }

  const handleBatchImport = async () => {
    // 先单独解析 JSON，给出精准的错误提示
    let credentials: CredentialInput[]
    try {
      const parsed = JSON.parse(jsonInput)
      credentials = Array.isArray(parsed) ? parsed : [parsed]
    } catch (error) {
      toast.error(t('jsonInvalid', { message: extractErrorMessage(error) }))
      return
    }

    if (credentials.length === 0) {
      toast.error(t('noCredentialToImport'))
      return
    }

    try {
      setImporting(true)
      setProgress({ current: 0, total: credentials.length })

      // 2. 初始化结果
      const initialResults: VerificationResult[] = credentials.map((_, i) => ({
        index: i + 1,
        status: 'pending'
      }))
      setResults(initialResults)

      // 3. 检测重复
      const existingTokenHashes = new Set(
        existingCredentials?.credentials
          .map(c => c.refreshTokenHash)
          .filter((hash): hash is string => Boolean(hash)) || []
      )

      let successCount = 0
      let duplicateCount = 0
      let failCount = 0
      let rollbackSuccessCount = 0
      let rollbackFailedCount = 0
      let rollbackSkippedCount = 0

      // 4. 导入并验活
      for (let i = 0; i < credentials.length; i++) {
        const cred = credentials[i]
        const token = cred.refreshToken.trim()
        const tokenHash = await sha256Hex(token)

        // 更新状态为检查中
        setCurrentProcessing(t('processingCredential', { current: i + 1, total: credentials.length }))
        setResults(prev => {
          const newResults = [...prev]
          newResults[i] = { ...newResults[i], status: 'checking' }
          return newResults
        })

        // 检查重复
        if (existingTokenHashes.has(tokenHash)) {
          duplicateCount++
          const existingCred = existingCredentials?.credentials.find(c => c.refreshTokenHash === tokenHash)
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'duplicate',
              error: t('credentialExists'),
              email: existingCred?.email || undefined
            }
            return newResults
          })
          setProgress({ current: i + 1, total: credentials.length })
          continue
        }

        // 更新状态为验活中
        setResults(prev => {
          const newResults = [...prev]
          newResults[i] = { ...newResults[i], status: 'verifying' }
          return newResults
        })

        let addedCredId: number | null = null

        try {
          // 添加凭据
          const clientId = cred.clientId?.trim() || undefined
          const clientSecret = cred.clientSecret?.trim() || undefined
          const authMethod = clientId && clientSecret ? 'idc' : 'social'

          // idc 模式下必须同时提供 clientId 和 clientSecret
          if (authMethod === 'social' && (clientId || clientSecret)) {
            throw new Error('idc mode requires both clientId and clientSecret')
          }

          const addedCred = await addCredential({
            refreshToken: token,
            authMethod,
            authRegion: cred.authRegion?.trim() || cred.region?.trim() || undefined,
            apiRegion: cred.apiRegion?.trim() || undefined,
            clientId,
            clientSecret,
            profileArn: cred.profileArn?.trim() || undefined,
            priority: cred.priority || 0,
            machineId: cred.machineId?.trim() || undefined,
          })

          addedCredId = addedCred.credentialId

          // 延迟 1 秒
          await new Promise(resolve => setTimeout(resolve, 1000))

          // 验活
          const balance = await getCredentialBalance(addedCred.credentialId)

          // 验活成功
          successCount++
          existingTokenHashes.add(tokenHash)
          setCurrentProcessing(addedCred.email
            ? t('verifySuccessWithName', { name: addedCred.email })
            : t('verifySuccessWithCredentialIndex', { index: i + 1 }))
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'verified',
              usage: `${balance.currentUsage}/${balance.usageLimit}`,
              email: addedCred.email || undefined,
              credentialId: addedCred.credentialId
            }
            return newResults
          })
        } catch (error) {
          // 验活失败，尝试回滚（先禁用再删除）
          let rollbackStatus: VerificationResult['rollbackStatus'] = 'skipped'
          let rollbackError: string | undefined

          if (addedCredId) {
            const rollbackResult = await rollbackCredential(addedCredId)
            if (rollbackResult.success) {
              rollbackStatus = 'success'
              rollbackSuccessCount++
            } else {
              rollbackStatus = 'failed'
              rollbackFailedCount++
              rollbackError = rollbackResult.error
            }
          } else {
            rollbackSkippedCount++
          }

          failCount++
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'failed',
              error: extractErrorMessage(error),
              email: undefined,
              rollbackStatus,
              rollbackError,
            }
            return newResults
          })
        }

        setProgress({ current: i + 1, total: credentials.length })
      }

      // 显示结果
      if (failCount === 0 && duplicateCount === 0) {
        toast.success(t('importAndVerifySuccess', { count: successCount }))
      } else {
        const failureSummary = failCount > 0
          ? t('verifyFailureSummary', {
            failed: failCount,
            rollbackSuccess: rollbackSuccessCount,
            rollbackFailed: rollbackFailedCount,
            rollbackSkipped: rollbackSkippedCount,
          })
          : ''
        toast.info(t('verifySummary', { success: successCount, duplicate: duplicateCount, failureSummary }))

        if (rollbackFailedCount > 0) {
          toast.warning(t('rollbackManualWarning', { count: rollbackFailedCount }))
        }
      }
    } catch (error) {
      toast.error(t('importFailed', { message: extractErrorMessage(error) }))
    } finally {
      setImporting(false)
    }
  }

  const getStatusIcon = (status: VerificationResult['status']) => {
    switch (status) {
      case 'pending':
        return <div className="w-5 h-5 rounded-full border-2 border-gray-300" />
      case 'checking':
      case 'verifying':
        return <Loader2 className="w-5 h-5 animate-spin text-blue-500" />
      case 'verified':
        return <CheckCircle2 className="w-5 h-5 text-green-500" />
      case 'duplicate':
        return <AlertCircle className="w-5 h-5 text-yellow-500" />
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-500" />
    }
  }

  const getStatusText = (result: VerificationResult) => {
    switch (result.status) {
      case 'pending':
        return t('waiting')
      case 'checking':
        return t('checkingDuplicate')
      case 'verifying':
        return t('verifyingShort')
      case 'verified':
        return t('verified')
      case 'duplicate':
        return t('duplicate')
      case 'failed':
        if (result.rollbackStatus === 'success') return t('verifyFailedExcluded')
        if (result.rollbackStatus === 'failed') return t('verifyFailedNotExcluded')
        return t('verifyFailedNotCreated')
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(newOpen) => {
        // 关闭时清空表单（但不在导入过程中清空）
        if (!newOpen && !importing) {
          resetForm()
        }
        onOpenChange(newOpen)
      }}
    >
      <DialogContent className="sm:max-w-2xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>{t('batchImportTitle')}</DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 py-4">
          <div className="space-y-2">
            <label className="text-sm font-medium">
              {t('jsonCredentialLabel')}
            </label>
            <textarea
              placeholder={t('jsonCredentialsPlaceholder')}
              value={jsonInput}
              onChange={(e) => setJsonInput(e.target.value)}
              disabled={importing}
              className="flex min-h-[200px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 font-mono"
            />
              <p className="text-xs text-muted-foreground">
                {t('autoVerifyHint')}
              </p>
            </div>

          {(importing || results.length > 0) && (
            <>
              {/* 进度条 */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>{importing ? t('verifyProgressLabel') : t('verifyCompletedLabel')}</span>
                  <span>{progress.current} / {progress.total}</span>
                </div>
                <div className="w-full bg-secondary rounded-full h-2">
                  <div
                    className="bg-primary h-2 rounded-full transition-all"
                    style={{ width: `${(progress.current / progress.total) * 100}%` }}
                  />
                </div>
                {importing && currentProcessing && (
                  <div className="text-xs text-muted-foreground">
                    {currentProcessing}
                  </div>
                )}
              </div>

              {/* 统计 */}
              <div className="flex gap-4 text-sm">
                <span className="text-green-600 dark:text-green-400">
                  {t('success')}: {results.filter(r => r.status === 'verified').length}
                </span>
                <span className="text-yellow-600 dark:text-yellow-400">
                  {t('duplicate')}: {results.filter(r => r.status === 'duplicate').length}
                </span>
                <span className="text-red-600 dark:text-red-400">
                  {t('failed')}: {results.filter(r => r.status === 'failed').length}
                </span>
              </div>

              {/* 结果列表 */}
              <div className="border rounded-md divide-y max-h-[300px] overflow-y-auto">
                {results.map((result) => (
                  <div key={result.index} className="p-3">
                    <div className="flex items-start gap-3">
                      {getStatusIcon(result.status)}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium">
                            {result.email || t('credentialId', { id: result.index })}
                          </span>
                          <span className="text-xs text-muted-foreground">
                            {getStatusText(result)}
                          </span>
                        </div>
                        {result.usage && (
                          <div className="text-xs text-muted-foreground mt-1">
                            {t('usage', { value: result.usage })}
                          </div>
                        )}
                        {result.error && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                            {result.error}
                          </div>
                        )}
                        {result.rollbackError && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                            {t('rollbackFailed', { message: result.rollbackError })}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              onOpenChange(false)
              resetForm()
            }}
            disabled={importing}
          >
              {importing ? t('verifyingShort') : results.length > 0 ? t('close') : t('cancel')}
            </Button>
          {results.length === 0 && (
            <Button
              type="button"
              onClick={handleBatchImport}
              disabled={importing || !jsonInput.trim()}
            >
                {t('startImportVerify')}
              </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
